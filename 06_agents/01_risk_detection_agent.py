"""
06_agents / 01_risk_detection_agent.py
========================================
Agent de détection du risque de churn (ReAct + adaptive thinking).
Lit gold.agg_churn_predictions, analyse chaque entreprise High Risk
via Claude Opus 4.6 (Databricks Model Serving) avec tool_use et
thinking adaptatif, puis déclenche le RetentionActionAgent.

Prérequis Databricks :
    - Endpoint Model Serving configuré (External Model → Anthropic, ou FMAPI)
    - Authentification : DATABRICKS_HOST + DATABRICKS_TOKEN (ou profil CLI)
    - Aucune ANTHROPIC_API_KEY nécessaire dans le code

Exécution :
    python 06_agents/01_risk_detection_agent.py
    python 06_agents/01_risk_detection_agent.py --tier High --mrr-min 2000
    python 06_agents/01_risk_detection_agent.py --dry-run
"""

import argparse
import json
import sys
import time
import uuid
from pathlib import Path
from datetime import datetime, timezone

import yaml

try:
    _SCRIPT_DIR = Path(__file__).parent
except NameError:
    _SCRIPT_DIR = Path("/Workspace/Shared/novacrm/06_agents")
_CONFIG_PATH = _SCRIPT_DIR.parent / "config.yml"

with open(_CONFIG_PATH) as f:
    _CONFIG = yaml.safe_load(f)

CATALOG        = _CONFIG["catalog"]
WAREHOUSE_ID   = _CONFIG.get("warehouse_id", "auto")
PROFILE        = _CONFIG.get("databricks_profile", "DEFAULT")
SQL_TIMEOUT    = _CONFIG.get("sql_timeout", 300)

AGENT_NAME     = "RiskDetectionAgent"
RISK_THRESHOLD = 0.70

# Endpoint Databricks Foundation Model APIs (pas de secret requis)
MODEL_ENDPOINT = _CONFIG.get("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")

PREDICTIONS_TABLE = f"{CATALOG}.gold.agg_churn_predictions"
HEALTH_TABLE      = f"{CATALOG}.gold.agg_company_health_score"

SYSTEM_PROMPT = f"""Tu es le RiskDetectionAgent de NovaCRM Solutions.
Ton rôle est d'analyser les prédictions de churn et de décider quelle action
de rétention déclencher pour chaque entreprise à risque.

Pour chaque entreprise :
1. Appelle get_company_context(company_id) pour charger le profil complet
2. Identifie les 3 principaux signaux d'alerte (usage, support, NPS, contrat)
3. Choisis le type d'action le plus approprié :
   - "email"      : risque modéré, relation normale, NPS neutre
   - "call"       : risque élevé, historique tickets, contact décideur disponible
   - "discount"   : risque élevé + renouvellement < 60 jours ou déjà downgrade
   - "escalation" : MRR > 5 000 EUR ou client très haute valeur
4. Retourne une décision structurée avec le type d'action et la justification

Sois factuel et concis. Tes décisions alimentent directement le RetentionActionAgent.
Priorité absolue aux entreprises avec MRR > 5 000 EUR.
"""


# ── Databricks SDK helpers ─────────────────────────────────────────────────────

def _get_db_client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(profile=PROFILE if PROFILE != "DEFAULT" else None)


def _get_deploy_client():
    """Client mlflow.deployments pour appeler Databricks Model Serving."""
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


RETENTION_TABLE = f"{CATALOG}.gold.agg_retention_actions"
_CHANNEL_MAP = {"email": "email", "call": "phone",
                "discount": "email", "escalation": "in_app"}


def write_decisions_to_gold(db_client, warehouse_id: str, decisions: list) -> int:
    """
    Écrit les décisions dans gold.agg_retention_actions avec status='queued'.
    Le contenu généré (email, script, etc.) sera créé par RetentionActionAgent.
    Retourne le nombre de lignes insérées.
    """
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    n_written = 0

    for decision in decisions:
        def esc(s):
            return str(s).replace("'", "''") if s else ""

        action_id   = str(uuid.uuid4())
        company_id  = decision["company_id"]
        action_type = decision["action_type"]
        channel     = _CHANNEL_MAP.get(action_type, "email")
        # Stocke les métadonnées de décision en JSON pour que RetentionActionAgent les lise
        meta_json = json.dumps({
            "key_signals": decision.get("key_signals", []),
            "rationale":   decision.get("rationale", ""),
            "urgency":     decision.get("urgency", "high"),
            "churn_probability": decision.get("churn_probability", 0),
        }, ensure_ascii=False)

        merge_sql = f"""
        MERGE INTO {RETENTION_TABLE} AS target
        USING (
            SELECT
                '{esc(action_id)}'   AS action_id,
                '{esc(company_id)}'  AS company_id,
                ''                   AS contact_id,
                ''                   AS agent_id,
                '{esc(action_type)}' AS action_type,
                '{esc(channel)}'     AS channel,
                'queued'             AS status,
                '{esc(meta_json)}'   AS generated_content,
                CAST('{now_utc}' AS TIMESTAMP) AS created_at,
                NULL                 AS approved_at,
                NULL                 AS executed_at,
                'pending'            AS outcome,
                '{AGENT_NAME}'       AS created_by_agent
        ) AS source
        ON  target.company_id  = source.company_id
        AND target.action_type = source.action_type
        AND target.status      = 'queued'
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
        try:
            _execute_sql(db_client, warehouse_id, merge_sql)
            n_written += 1
        except Exception as e:
            print(f"  Erreur écriture {company_id}: {e}")

    return n_written


# ── Tool functions ─────────────────────────────────────────────────────────────

def get_high_risk_companies(db_client, warehouse_id: str,
                             prediction_date: str, tier: str, mrr_min: float,
                             max_companies: int):
    tier_filter = f"AND p.risk_tier = '{tier}'" if tier in ("High", "Medium") else ""

    query = f"""
    SELECT
        p.company_id, p.churn_probability, p.risk_tier, p.top_factors, p.model_version,
        dc.company_name, dc.sector_label AS industry, dc.company_size, c.mrr AS mrr_current,
        c.days_to_renewal, dc.assigned_cs_agent_id AS cs_agent_id,
        c.health_score, c.risk_tier AS health_risk_tier
    FROM {PREDICTIONS_TABLE} p
    JOIN {CATALOG}.silver.dim_companies dc ON p.company_id = dc.company_id
    JOIN {HEALTH_TABLE} c ON p.company_id = c.company_id
    WHERE p.prediction_date = '{prediction_date}'
      {tier_filter}
      AND CAST(c.mrr AS DOUBLE) >= {mrr_min}
    ORDER BY p.churn_probability DESC
    LIMIT {max_companies}
    """
    return _sql_to_df(db_client, warehouse_id, query)


def get_latest_prediction_date(db_client, warehouse_id: str) -> str:
    df = _sql_to_df(db_client, warehouse_id,
                    f"SELECT MAX(prediction_date) AS latest FROM {PREDICTIONS_TABLE}")
    if df.empty or df.iloc[0]["latest"] is None:
        raise RuntimeError(
            f"Aucune prédiction dans {PREDICTIONS_TABLE}. "
            "Exécutez d'abord 05_ml/03_batch_inference.py."
        )
    return str(df.iloc[0]["latest"])


def get_company_context(db_client, warehouse_id: str, company_id: str) -> dict:
    """Charge le contexte complet d'une entreprise pour l'analyse."""

    contacts_df = _sql_to_df(db_client, warehouse_id, f"""
        SELECT CONCAT(first_name, ' ', last_name) AS contact_name, role, email, phone
        FROM {CATALOG}.silver.dim_contacts
        WHERE company_id = '{company_id}'
          AND role IN ('CEO','CFO','CTO','VP Sales','VP Operations','Director')
        LIMIT 3
    """)
    tickets_df = _sql_to_df(db_client, warehouse_id, f"""
        SELECT priority, status, sentiment_score, is_escalated, subject
        FROM {CATALOG}.silver.fact_support_tickets
        WHERE company_id = '{company_id}'
        ORDER BY created_at DESC LIMIT 5
    """)
    nps_df = _sql_to_df(db_client, warehouse_id, f"""
        SELECT nps_score, nps_category, comment AS verbatim, survey_date
        FROM {CATALOG}.silver.fact_nps_scores
        WHERE company_id = '{company_id}'
        ORDER BY survey_date DESC LIMIT 2
    """)
    sub_df = _sql_to_df(db_client, warehouse_id, f"""
        SELECT plan_type AS plan_name, mrr, billing_cycle, end_date AS contract_end_date, status
        FROM {CATALOG}.silver.fact_subscriptions
        WHERE company_id = '{company_id}' AND is_current_contract = TRUE LIMIT 1
    """)
    agent_df = _sql_to_df(db_client, warehouse_id, f"""
        SELECT CONCAT(a.first_name, ' ', a.last_name) AS agent_name, a.region, a.email AS agent_email
        FROM {CATALOG}.silver.dim_cs_agents a
        WHERE a.agent_id = (
            SELECT assigned_cs_agent_id FROM {CATALOG}.silver.dim_companies WHERE company_id = '{company_id}'
        ) LIMIT 1
    """)

    def to_list(df):
        return df.to_dict(orient="records") if not df.empty else []

    return {
        "contacts":       to_list(contacts_df),
        "recent_tickets": to_list(tickets_df),
        "nps_history":    to_list(nps_df),
        "subscription":   to_list(sub_df),
        "cs_agent":       to_list(agent_df),
    }


# ── Définition des outils (format OpenAI function calling) ────────────────────

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_company_context",
            "description": (
                "Charge le contexte complet d'une entreprise cliente : "
                "contacts décideurs, derniers tickets de support, historique NPS, "
                "contrat en cours et agent CS assigné. "
                "Appelle AVANT de décider une action."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "company_id": {
                        "type": "string",
                        "description": "Identifiant unique de l'entreprise"
                    }
                },
                "required": ["company_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "decide_action",
            "description": (
                "Enregistre la décision finale d'action de rétention. "
                "Appelle APRÈS avoir analysé le contexte."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "company_id":  {"type": "string"},
                    "action_type": {
                        "type": "string",
                        "enum": ["email", "call", "discount", "escalation"]
                    },
                    "key_signals": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "3 principaux signaux d'alerte"
                    },
                    "rationale": {
                        "type": "string",
                        "description": "Justification courte (1-2 phrases)"
                    },
                    "urgency": {
                        "type": "string",
                        "enum": ["critical", "high", "medium"]
                    }
                },
                "required": ["company_id", "action_type", "key_signals", "rationale", "urgency"]
            }
        }
    }
]


# ── Boucle ReAct avec Databricks Model Serving ────────────────────────────────

def analyze_company(deploy_client, db_client, warehouse_id: str,
                    company_row: dict) -> dict | None:
    """
    Boucle ReAct Claude Opus 4.6 via Databricks Model Serving (adaptive thinking).
    Retourne la décision (dict) ou None en cas d'échec.
    """
    import mlflow

    company_id   = company_row["company_id"]
    company_name = company_row.get("company_name", company_id)
    churn_prob   = float(company_row.get("churn_probability", 0))
    mrr          = float(company_row.get("mrr_current", 0))
    top_factors  = company_row.get("top_factors", "[]")

    user_message = (
        f"Analyse l'entreprise suivante et décide de l'action de rétention :\n\n"
        f"- company_id       : {company_id}\n"
        f"- Nom              : {company_name}\n"
        f"- churn_probability: {churn_prob:.1%}\n"
        f"- risk_tier        : {company_row.get('risk_tier', 'High')}\n"
        f"- MRR              : {mrr:,.0f} EUR\n"
        f"- Jours renouvellement : {company_row.get('days_to_renewal', '?')}\n"
        f"- Top SHAP factors : {top_factors}\n"
        f"- Health score     : {company_row.get('health_score', '?')}\n\n"
        f"Commence par appeler get_company_context('{company_id}') "
        f"puis decide_action()."
    )

    # Format OpenAI : system en premier message
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": user_message},
    ]
    decision = None

    with mlflow.start_span(name=f"analyze_{company_id}") as span:
        span.set_attribute("company_id", company_id)
        span.set_attribute("churn_probability", churn_prob)
        span.set_attribute("mrr", mrr)

        for turn in range(6):  # max 6 tours
            response = deploy_client.predict(
                endpoint=MODEL_ENDPOINT,
                inputs={
                    "messages": messages,
                    "tools":    TOOLS,
                    "max_tokens": 4096,
                }
            )

            # Format OpenAI : choices[0].message
            choices      = response.get("choices", [])
            if not choices:
                break
            choice        = choices[0]
            message       = choice.get("message", {})
            finish_reason = choice.get("finish_reason", "stop")

            # Ajouter la réponse assistant à la conversation
            messages.append(message)

            if finish_reason == "stop":
                break

            if finish_reason != "tool_calls":
                break

            # Traitement des tool_calls (format OpenAI)
            tool_calls = message.get("tool_calls", []) or []
            for tc in tool_calls:
                tool_name = tc["function"]["name"]
                try:
                    tool_input = json.loads(tc["function"]["arguments"])
                except Exception:
                    tool_input = {}
                tool_call_id = tc["id"]

                if tool_name == "get_company_context":
                    try:
                        ctx = get_company_context(
                            db_client, warehouse_id, tool_input["company_id"]
                        )
                        result_content = json.dumps(ctx, default=str, ensure_ascii=False)
                    except Exception as e:
                        result_content = f"Erreur: {e}"

                elif tool_name == "decide_action":
                    decision = {
                        "company_id":        tool_input["company_id"],
                        "action_type":       tool_input["action_type"],
                        "key_signals":       tool_input.get("key_signals", []),
                        "rationale":         tool_input.get("rationale", ""),
                        "urgency":           tool_input.get("urgency", "high"),
                        "churn_probability": churn_prob,
                        "mrr":               mrr,
                        "company_name":      company_name,
                    }
                    result_content = (
                        f"Décision enregistrée : {tool_input['action_type']} "
                        f"(urgency={tool_input.get('urgency','high')})"
                    )

                else:
                    result_content = f"Outil inconnu : {tool_name}"

                # Format OpenAI : role=tool, tool_call_id
                messages.append({
                    "role":         "tool",
                    "tool_call_id": tool_call_id,
                    "content":      result_content,
                })

            if decision is not None:
                break

        if decision:
            span.set_attribute("action_type", decision["action_type"])
            span.set_attribute("urgency",     decision["urgency"])

    return decision


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=f"NovaCRM — {AGENT_NAME}")
    parser.add_argument("--date",          default=None)
    parser.add_argument("--tier",          default="High", choices=["High","Medium","All"])
    parser.add_argument("--mrr-min",       type=float, default=0.0)
    parser.add_argument("--max-companies", type=int,   default=50)
    parser.add_argument("--dry-run",       action="store_true")
    args = parser.parse_args()

    print("=" * 65)
    print(f" NovaCRM — {AGENT_NAME}")
    print(f" Catalog  : {CATALOG}")
    print(f" Endpoint : {MODEL_ENDPOINT} (Databricks Model Serving)")
    print("=" * 65)

    import mlflow
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(f"/Shared/novacrm/{CATALOG}_agents")

    db_client    = _get_db_client()
    warehouse_id = _get_warehouse_id(db_client)
    deploy_client = _get_deploy_client()

    pred_date = args.date or get_latest_prediction_date(db_client, warehouse_id)
    print(f"\n  Date de prédiction : {pred_date}")
    print(f"  Tier filtré        : {args.tier}")
    print(f"  MRR minimum        : {args.mrr_min:,.0f} EUR")

    print(f"\n[1/3] Chargement des entreprises {args.tier} Risk...")
    companies_df = get_high_risk_companies(
        db_client, warehouse_id, pred_date,
        tier=args.tier, mrr_min=args.mrr_min, max_companies=args.max_companies
    )

    if companies_df.empty:
        print(f"  Aucune entreprise {args.tier} Risk trouvée pour {pred_date}.")
        sys.exit(0)

    print(f"  {len(companies_df)} entreprise(s) à analyser")

    print(f"\n[2/3] Analyse par {AGENT_NAME} (Claude Opus 4.6 + adaptive thinking)...")
    decisions = []

    with mlflow.start_run(run_name=f"{AGENT_NAME}_{pred_date}"):
        mlflow.log_param("prediction_date", pred_date)
        mlflow.log_param("tier_filter",     args.tier)
        mlflow.log_param("n_companies",     len(companies_df))
        mlflow.log_param("model_endpoint",  MODEL_ENDPOINT)
        mlflow.log_param("dry_run",         args.dry_run)

        for i, row in companies_df.iterrows():
            cid   = row["company_id"]
            cname = row.get("company_name", cid)
            prob  = float(row.get("churn_probability", 0))
            mrr   = float(row.get("mrr_current", 0))

            print(f"  [{i+1}/{len(companies_df)}] {cname} ({cid}) "
                  f"| prob={prob:.1%} | MRR={mrr:,.0f}€ ...",
                  end=" ", flush=True)

            try:
                decision = analyze_company(
                    deploy_client, db_client, warehouse_id, dict(row)
                )
                if decision:
                    decisions.append(decision)
                    print(f"→ {decision['action_type'].upper()} ({decision['urgency']})")
                else:
                    print("→ aucune décision")
            except Exception as e:
                print(f"→ ERREUR: {e}")

        mlflow.log_metric("n_decisions", len(decisions))
        for d in decisions:
            key = f"n_{d['action_type']}"
            try:
                mlflow.log_metric(key, mlflow.get_run(mlflow.active_run().info.run_id)
                                  .data.metrics.get(key, 0) + 1)
            except Exception:
                pass

    print(f"\n[3/3] Écriture des décisions dans gold.agg_retention_actions (status='queued')...")

    if not decisions:
        print("  Aucune décision — rien à écrire.")
        sys.exit(0)

    if args.dry_run:
        print(f"  [DRY RUN] {len(decisions)} actions à enregistrer (non exécuté)")
        _print_decisions_summary(decisions)
        sys.exit(0)

    n_written = write_decisions_to_gold(db_client, warehouse_id, decisions)

    print(f"\n{'=' * 65}")
    print(f"  ✓ {AGENT_NAME} terminé")
    print(f"  {len(decisions)} décisions | {n_written} lignes écrites (status='queued')")
    print(f"\n  Étape suivante :")
    print(f"    python 06_agents/02_retention_action_agent.py")
    print("=" * 65)


def _print_decisions_summary(decisions: list) -> None:
    print(f"\n  Résumé ({len(decisions)} entreprises) :")
    print(f"  {'Entreprise':<30} {'Action':<12} {'Urgence':<10} {'Prob':<8} {'MRR':>10}")
    print("  " + "-" * 75)
    for d in sorted(decisions, key=lambda x: x["churn_probability"], reverse=True):
        print(f"  {d.get('company_name', d['company_id']):<30} "
              f"{d['action_type']:<12} {d['urgency']:<10} "
              f"{d['churn_probability']:.1%}    "
              f"{d.get('mrr', 0):>10,.0f}€")


if __name__ == "__main__":
    main()
