"""
06_agents / 02_retention_action_agent.py
==========================================
Agent de génération d'actions de rétention personnalisées.
Utilise Claude Opus 4.6 via Databricks Model Serving (adaptive thinking)
pour générer du contenu sur mesure, puis l'insère dans
gold.agg_retention_actions avec status='pending'.

Prérequis Databricks :
    - Endpoint Model Serving configuré (External Model → Anthropic, ou FMAPI)
    - Authentification : DATABRICKS_HOST + DATABRICKS_TOKEN (ou profil CLI)
    - Aucune ANTHROPIC_API_KEY nécessaire dans le code

Exécution standalone :
    python 06_agents/02_retention_action_agent.py --company-id C001 --action-type email
"""

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml

try:
    _SCRIPT_DIR = Path(__file__).parent
except NameError:
    _SCRIPT_DIR = Path("/Workspace/Shared/novacrm/06_agents")
_CONFIG_PATH = _SCRIPT_DIR.parent / "config.yml"

with open(_CONFIG_PATH) as f:
    _CONFIG = yaml.safe_load(f)

CATALOG      = _CONFIG["catalog"]
WAREHOUSE_ID = _CONFIG.get("warehouse_id", "auto")
PROFILE      = _CONFIG.get("databricks_profile", "DEFAULT")
SQL_TIMEOUT  = _CONFIG.get("sql_timeout", 300)

AGENT_NAME   = "RetentionActionAgent"

# Endpoint Databricks Foundation Model APIs (pas de secret requis)
MODEL_ENDPOINT = _CONFIG.get("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")

OUTPUT_TABLE      = f"{CATALOG}.gold.agg_retention_actions"
PREDICTIONS_TABLE = f"{CATALOG}.gold.agg_churn_predictions"
HEALTH_TABLE      = f"{CATALOG}.gold.agg_company_health_score"

# ── System prompts par type d'action ──────────────────────────────────────────

SYSTEM_PROMPTS = {
    "email": """Tu es le RetentionActionAgent de NovaCRM Solutions.
Génère un email de rétention professionnel en français pour un client à risque de churn.

Structure OBLIGATOIRE :
OBJET: [ligne d'objet percutante]

[Corps de l'email]
- Accroche personnalisée avec un point positif de leur utilisation de NovaCRM
- Reconnaissance de leurs difficultés (sans nommer le churn)
- Proposition de valeur concrète (formation, session démo, optimisation workflow)
- Call-to-action clair avec créneaux proposés
- Signature de l'agent CS

Règles :
- Ton : professionnel, empathique, pas commercial
- Maximum 280 mots pour le corps
- Personnalise avec le prénom du contact et le nom de l'entreprise
- Inclus 1 statistique pertinente de leur usage si disponible
""",

    "call": """Tu es le RetentionActionAgent de NovaCRM Solutions.
Génère un script d'appel téléphonique pour un agent CS contactant un client à risque.

Structure OBLIGATOIRE :
DURÉE ESTIMÉE: [X minutes]

ACCROCHE (30s) :
[script verbatim]

DIAGNOSTIC (2-3 min) :
Questions clés à poser :
• [question 1]
• [question 2]
• [question 3]

PROPOSITION (2-3 min) :
[solutions à présenter selon les réponses]

CLOSING (1 min) :
[engagement / prochaine étape]

OBJECTIONS COURANTES :
[Objection] → [Réponse suggérée]

Règles :
- Naturel et conversationnel, pas robotique
- Commencer par remercier pour leur partenariat
- Maximum 200 mots par section
""",

    "discount": """Tu es le RetentionActionAgent de NovaCRM Solutions.
Génère une offre commerciale personnalisée pour retenir un client à risque.

Structure OBLIGATOIRE :
OFFRE PROPOSÉE:
• Type de réduction : [%remise / mois offerts / upgrade gratuit]
• Valeur : [montant EUR ou % exact]
• Durée : [période de validité de l'offre]
• Conditions : [engagement minimum si applicable]

EMAIL D'ACCOMPAGNEMENT :
OBJET: [objet accrocheur]

[Corps de l'email incluant l'offre, les bénéfices et le call-to-action]

JUSTIFICATION INTERNE (pour validation) :
• MRR sauvegardé : [montant]
• Coût de l'offre : [montant]
• Break-even : [délai]
• Risque si on ne fait rien : [MRR perdu]

Règles :
- L'offre doit être justifiée économiquement (LTV > coût rétention)
- Maximum 20% de réduction sans approbation manager
- Lier la réduction à un engagement (6 ou 12 mois)
""",

    "escalation": """Tu es le RetentionActionAgent de NovaCRM Solutions.
Génère un brief d'escalade vers le management pour un client à très haute valeur à risque.

Structure OBLIGATOIRE :
BRIEF ESCALADE — CONFIDENTIEL

CLIENT : [nom]
MRR : [montant] EUR/mois
URGENCE : [délai recommandé]

RÉSUMÉ EXÉCUTIF (3 phrases max) :
[situation critique, pourquoi escalader maintenant]

SIGNAUX D'ALERTE :
• [signal 1 avec données]
• [signal 2 avec données]
• [signal 3 avec données]

HISTORIQUE RELATION :
[tenure, incidents passés, points forts]

RECOMMANDATION :
Action suggérée : [appel CEO / réunion QBR / offre sur-mesure]
Délai maximum : [X jours]
Intervenant suggéré : [rôle NovaCRM à impliquer]

PRÉPARATION RÉUNION :
• Points à aborder : [liste]
• Concessions possibles : [liste]
• Ligne rouge (ne pas céder sur) : [liste]

Règles :
- Factuel et direct, sans optimisme excessif
- Inclure les données chiffrées (NPS, tickets, MRR trend)
- Maximum 400 mots total
""",
}


# ── Databricks helpers ─────────────────────────────────────────────────────────

def _get_db_client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(profile=PROFILE if PROFILE != "DEFAULT" else None)


def _get_deploy_client():
    from mlflow.deployments import get_deploy_client
    return get_deploy_client("databricks")


def _get_warehouse_id(client) -> str:
    if WAREHOUSE_ID and WAREHOUSE_ID != "auto":
        return WAREHOUSE_ID
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("Aucun SQL Warehouse disponible.")
    running = [w for w in warehouses if "RUNNING" in str(w.state)]
    chosen = running[0] if running else warehouses[0]
    print(f"  Warehouse : {chosen.name} (id={chosen.id})")
    return chosen.id


def _sql_to_df(client, warehouse_id: str, query: str):
    import pandas as pd
    from databricks.sdk.service.sql import StatementState

    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=query,
        wait_timeout="50s",
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = client.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error
        raise RuntimeError(f"SQL failed: {err.message if err else 'unknown'}")

    col_names = [c.name for c in resp.manifest.schema.columns]
    rows      = resp.result.data_array or []
    return pd.DataFrame(rows, columns=col_names)


def _execute_sql(client, warehouse_id: str, statement: str) -> None:
    from databricks.sdk.service.sql import StatementState

    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = client.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error
        raise RuntimeError(f"SQL failed: {err.message if err else 'unknown'}")


# ── Chargement du contexte entreprise ─────────────────────────────────────────

def load_company_context(db_client, warehouse_id: str, company_id: str) -> dict:
    company_df = _sql_to_df(db_client, warehouse_id, f"""
        SELECT dc.company_id, dc.company_name, dc.sector_label AS industry, dc.company_size,
               dc.city, dc.country,
               h.mrr AS mrr_current, h.days_to_renewal, h.health_score,
               h.risk_tier, dc.assigned_cs_agent_id AS cs_agent_id
        FROM {CATALOG}.silver.dim_companies dc
        JOIN {HEALTH_TABLE} h ON dc.company_id = h.company_id
        WHERE dc.company_id = '{company_id}'
    """)
    if company_df.empty:
        raise ValueError(f"Entreprise {company_id} introuvable.")
    company = company_df.iloc[0].to_dict()

    contact_df = _sql_to_df(db_client, warehouse_id, f"""
        SELECT contact_id, CONCAT(first_name, ' ', last_name) AS contact_name, role, email, phone
        FROM {CATALOG}.silver.dim_contacts
        WHERE company_id = '{company_id}'
          AND role IN ('CEO','CFO','CTO','VP Sales','VP Operations','Director')
        ORDER BY CASE role
            WHEN 'CEO' THEN 1 WHEN 'CFO' THEN 2 WHEN 'CTO' THEN 3
            WHEN 'VP Sales' THEN 4 WHEN 'VP Operations' THEN 5 ELSE 6 END
        LIMIT 1
    """)
    contact = contact_df.iloc[0].to_dict() if not contact_df.empty else {}

    nps_df = _sql_to_df(db_client, warehouse_id, f"""
        SELECT nps_score, nps_category, comment AS verbatim, survey_date
        FROM {CATALOG}.silver.fact_nps_scores
        WHERE company_id = '{company_id}'
        ORDER BY survey_date DESC LIMIT 2
    """)

    tickets_df = _sql_to_df(db_client, warehouse_id, f"""
        SELECT priority, status, subject, sentiment_score, is_escalated, created_at AS date_created
        FROM {CATALOG}.silver.fact_support_tickets
        WHERE company_id = '{company_id}'
        ORDER BY created_at DESC LIMIT 3
    """)

    sub_df = _sql_to_df(db_client, warehouse_id, f"""
        SELECT plan_type AS plan_name, mrr, billing_cycle,
               start_date AS contract_start_date, end_date AS contract_end_date,
               status, duration_days
        FROM {CATALOG}.silver.fact_subscriptions
        WHERE company_id = '{company_id}' AND is_current_contract = TRUE LIMIT 1
    """)

    cs_agent_df = _sql_to_df(db_client, warehouse_id, f"""
        SELECT a.agent_id, CONCAT(a.first_name, ' ', a.last_name) AS agent_name,
               a.region, a.email AS agent_email
        FROM {CATALOG}.silver.dim_cs_agents a
        WHERE a.agent_id = '{company.get("cs_agent_id", "")}'
        LIMIT 1
    """)

    pred_df = _sql_to_df(db_client, warehouse_id, f"""
        SELECT churn_probability, risk_tier, top_factors, prediction_date
        FROM {PREDICTIONS_TABLE}
        WHERE company_id = '{company_id}'
        ORDER BY prediction_date DESC LIMIT 1
    """)

    def to_list(df):
        return df.to_dict(orient="records") if not df.empty else []

    return {
        "company":        company,
        "contact":        contact,
        "nps_history":    to_list(nps_df),
        "recent_tickets": to_list(tickets_df),
        "subscription":   sub_df.iloc[0].to_dict() if not sub_df.empty else {},
        "cs_agent":       cs_agent_df.iloc[0].to_dict() if not cs_agent_df.empty else {},
        "prediction":     pred_df.iloc[0].to_dict() if not pred_df.empty else {},
    }


# ── Génération du contenu par Claude Opus 4.6 (Databricks) ───────────────────

def generate_content(deploy_client, context: dict, action_type: str,
                     key_signals: list = None, rationale: str = "") -> str:
    """
    Appelle Claude Opus 4.6 via Databricks Model Serving avec adaptive thinking.
    Retourne le texte généré (email, script, offre, brief).
    """
    import mlflow

    company  = context["company"]
    contact  = context.get("contact", {})
    nps      = context.get("nps_history", [{}])[0] if context.get("nps_history") else {}
    sub      = context.get("subscription", {})
    cs_agent = context.get("cs_agent", {})
    pred     = context.get("prediction", {})
    tickets  = context.get("recent_tickets", [])

    user_prompt = f"""Génère une action de type "{action_type}" pour ce client :

ENTREPRISE :
- Nom         : {company.get("company_name", "N/A")}
- Secteur     : {company.get("industry", "N/A")}
- Taille      : {company.get("company_size", "N/A")}
- Ville       : {company.get("city", "N/A")}

CONTACT PRINCIPAL :
- Prénom/Nom  : {contact.get("contact_name", "N/A")}
- Rôle        : {contact.get("role", "N/A")}
- Email       : {contact.get("email", "N/A")}

DONNÉES FINANCIÈRES :
- MRR         : {sub.get("mrr", company.get("mrr_current", "N/A"))} EUR/mois
- Plan        : {sub.get("plan_name", "N/A")} ({sub.get("billing_cycle", "N/A")})
- Renouvellement dans : {company.get("days_to_renewal", "N/A")} jours

SIGNAUX DE RISQUE :
- Churn probability : {pred.get("churn_probability", "N/A")}
- Dernier NPS       : {nps.get("nps_score", "N/A")} ({nps.get("nps_category", "N/A")})
- Verbatim NPS      : {nps.get("verbatim", "N/A")}
- Tickets récents   : {len(tickets)} ouvert(s)
- Signaux principaux: {json.dumps(key_signals or [], ensure_ascii=False)}

CONTEXTE ADDITIONNEL :
{rationale}

AGENT CS ASSIGNÉ :
- Nom   : {cs_agent.get("agent_name", "Votre agent CS NovaCRM")}
- Email : {cs_agent.get("agent_email", "support@novacrm.fr")}

Génère le contenu complet selon le format demandé dans les instructions système.
"""

    with mlflow.start_span(name=f"generate_{action_type}") as span:
        span.set_attribute("company_id",  company.get("company_id", ""))
        span.set_attribute("action_type", action_type)

        # Format OpenAI : system en premier message
        response = deploy_client.predict(
            endpoint=MODEL_ENDPOINT,
            inputs={
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPTS[action_type]},
                    {"role": "user",   "content": user_prompt},
                ],
                "max_tokens": 4096,
            }
        )

        # Parsing format OpenAI : choices[0].message.content
        choices = response.get("choices", [])
        content = choices[0]["message"]["content"].strip() if choices else ""

        usage = response.get("usage", {})
        span.set_attribute("output_length",   len(content))
        span.set_attribute("input_tokens",    usage.get("prompt_tokens", 0))
        span.set_attribute("output_tokens",   usage.get("completion_tokens", 0))

    return content


# ── Écriture dans gold.agg_retention_actions (MERGE idempotent) ───────────────

def write_action(db_client, warehouse_id: str, action: dict) -> None:
    def esc(s):
        return str(s).replace("'", "''") if s else ""

    merge_sql = f"""
    MERGE INTO {OUTPUT_TABLE} AS target
    USING (
        SELECT
            '{esc(action["action_id"])}'          AS action_id,
            '{esc(action["company_id"])}'         AS company_id,
            '{esc(action.get("contact_id",""))}'  AS contact_id,
            '{esc(action.get("agent_id",""))}'    AS agent_id,
            '{esc(action["action_type"])}'        AS action_type,
            '{esc(action["channel"])}'            AS channel,
            '{esc(action["status"])}'             AS status,
            '{esc(action["generated_content"])}' AS generated_content,
            CAST('{action["created_at"]}' AS TIMESTAMP) AS created_at,
            NULL                                  AS approved_at,
            NULL                                  AS executed_at,
            'pending'                             AS outcome,
            '{esc(action["created_by_agent"])}'  AS created_by_agent
    ) AS source
    ON target.action_id = source.action_id
    WHEN MATCHED THEN UPDATE SET
        target.generated_content = source.generated_content,
        target.status            = source.status
    WHEN NOT MATCHED THEN INSERT (
        action_id, company_id, contact_id, agent_id, action_type, channel,
        status, generated_content, created_at, approved_at, executed_at,
        outcome, created_by_agent
    ) VALUES (
        source.action_id, source.company_id, source.contact_id, source.agent_id,
        source.action_type, source.channel, source.status, source.generated_content,
        source.created_at, source.approved_at, source.executed_at,
        source.outcome, source.created_by_agent
    )
    """
    _execute_sql(db_client, warehouse_id, merge_sql)


# ── Traitement d'une décision ──────────────────────────────────────────────────

def process_decision(db_client, warehouse_id: str, deploy_client,
                     decision: dict) -> dict | None:
    company_id  = decision["company_id"]
    action_type = decision["action_type"]
    channel_map = {"email": "email", "call": "phone",
                   "discount": "email", "escalation": "in_app"}

    try:
        context = load_company_context(db_client, warehouse_id, company_id)
    except Exception as e:
        print(f"    Erreur contexte {company_id}: {e}")
        return None

    try:
        content = generate_content(
            deploy_client=deploy_client,
            context=context,
            action_type=action_type,
            key_signals=decision.get("key_signals", []),
            rationale=decision.get("rationale", ""),
        )
    except Exception as e:
        print(f"    Erreur génération {company_id}: {e}")
        return None

    contact  = context.get("contact", {})
    cs_agent = context.get("cs_agent", {})
    now_utc  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    action = {
        "action_id":         str(uuid.uuid4()),
        "company_id":        company_id,
        "contact_id":        contact.get("contact_id", ""),
        "agent_id":          cs_agent.get("agent_id", ""),
        "action_type":       action_type,
        "channel":           channel_map.get(action_type, "email"),
        "status":            "pending",
        "generated_content": content,
        "created_at":        now_utc,
        "created_by_agent":  AGENT_NAME,
    }

    try:
        write_action(db_client, warehouse_id, action)
    except Exception as e:
        print(f"    Erreur écriture {company_id}: {e}")
        return None

    return action


# ── Point d'entrée batch (appelé par RiskDetectionAgent) ─────────────────────

def run_retention_batch(db_client, warehouse_id: str, deploy_client,
                        decisions: list) -> int:
    """Traite une liste de décisions. Retourne le nombre d'actions générées."""
    import mlflow

    n_ok = 0
    with mlflow.start_run(run_name=f"{AGENT_NAME}_batch", nested=True):
        mlflow.log_param("n_input_decisions", len(decisions))
        mlflow.log_param("model_endpoint",    MODEL_ENDPOINT)

        for i, decision in enumerate(decisions):
            cid   = decision["company_id"]
            atype = decision["action_type"]
            print(f"  [{i+1}/{len(decisions)}] {cid} → {atype.upper()} ...",
                  end=" ", flush=True)

            action = process_decision(db_client, warehouse_id, deploy_client, decision)
            if action:
                n_ok += 1
                print("✓")
            else:
                print("✗")

        mlflow.log_metric("n_actions_generated", n_ok)

    return n_ok


# ── Main batch (lit status='queued', génère contenu, passe à 'pending') ────────

def main():
    parser = argparse.ArgumentParser(description=f"NovaCRM — {AGENT_NAME}")
    parser.add_argument("--dry-run", action="store_true",
                        help="Afficher les lignes queued sans générer")
    args = parser.parse_args()

    print("=" * 65)
    print(f" NovaCRM — {AGENT_NAME}")
    print(f" Endpoint : {MODEL_ENDPOINT} (Databricks Foundation Model APIs)")
    print("=" * 65)

    import mlflow
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(f"/Shared/novacrm/{CATALOG}_agents")

    db_client     = _get_db_client()
    warehouse_id  = _get_warehouse_id(db_client)
    deploy_client = _get_deploy_client()

    print(f"\n[1/3] Lecture des actions en attente (status='queued')...")
    queued_df = _sql_to_df(db_client, warehouse_id, f"""
        SELECT action_id, company_id, action_type, generated_content
        FROM {OUTPUT_TABLE}
        WHERE status = 'queued'
        ORDER BY created_at
    """)

    if queued_df.empty:
        print("  Aucune action en attente (status='queued'). Pipeline déjà traité ?")
        sys.exit(0)

    print(f"  {len(queued_df)} action(s) à générer")

    if args.dry_run:
        print(f"\n  [DRY RUN] Aperçu des actions queued :")
        for _, row in queued_df.iterrows():
            print(f"    {row['company_id']} → {row['action_type']}")
        sys.exit(0)

    print(f"\n[2/3] Génération du contenu par {AGENT_NAME}...")
    n_ok = 0

    with mlflow.start_run(run_name=f"{AGENT_NAME}_batch"):
        mlflow.log_param("n_queued",       len(queued_df))
        mlflow.log_param("model_endpoint", MODEL_ENDPOINT)

        for i, row in queued_df.iterrows():
            action_id   = row["action_id"]
            company_id  = row["company_id"]
            action_type = row["action_type"]

            print(f"  [{n_ok+1}/{len(queued_df)}] {company_id} → {action_type.upper()} ...",
                  end=" ", flush=True)

            # Récupérer les métadonnées de décision stockées en JSON
            try:
                meta = json.loads(row.get("generated_content") or "{}")
                key_signals = meta.get("key_signals", [])
                rationale   = meta.get("rationale", "")
            except Exception:
                key_signals, rationale = [], ""

            decision = {
                "company_id":  company_id,
                "action_type": action_type,
                "key_signals": key_signals,
                "rationale":   rationale,
            }

            try:
                action = process_decision(db_client, warehouse_id, deploy_client, decision)
            except Exception as e:
                print(f"✗ ERREUR: {e}")
                continue

            if action:
                # L'action a déjà été écrite par process_decision (status='pending')
                # Supprimer la ligne 'queued' maintenant remplacée
                def esc(s):
                    return str(s).replace("'", "''") if s else ""
                try:
                    _execute_sql(db_client, warehouse_id, f"""
                        DELETE FROM {OUTPUT_TABLE}
                        WHERE action_id = '{esc(action_id)}'
                    """)
                except Exception:
                    pass
                n_ok += 1
                print("✓")
            else:
                print("✗")

        mlflow.log_metric("n_actions_generated", n_ok)

    print(f"\n[3/3] Résumé")
    print(f"\n{'=' * 65}")
    print(f"  ✓ {AGENT_NAME} terminé")
    print(f"  {n_ok}/{len(queued_df)} actions générées (status='pending')")
    print(f"\n  Étape suivante :")
    print(f"    python 06_agents/03_approval_workflow.py")
    print("=" * 65)


if __name__ == "__main__":
    main()
