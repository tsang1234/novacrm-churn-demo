"""
06_agents / 03_approval_workflow.py
=====================================
Workflow Human-in-the-Loop pour la validation des actions de rétention.
Interface CLI interactive : présente les actions "pending" générées par
RetentionActionAgent et permet à l'agent CS de les Approuver / Modifier / Rejeter.

Exécution :
    python 06_agents/03_approval_workflow.py

Options :
    --agent-id AG001           Filtrer par agent CS (portefeuille)
    --tier High                Filtrer par risk_tier (High / Medium / All)
    --limit 10                 Nombre max d'actions à présenter
    --non-interactive          Mode batch : approuve tout (pour les tests)
    --reject-all               Mode batch : rejette tout (pour les tests)

Workflow des statuts :
    pending → approved  → sent → completed (outcome: saved / churned)
           → rejected   (action abandonnée, motif loggué)
           → modified   → pending (régénération avec instructions CS)
"""

import argparse
import json
import sys
import time
import textwrap
from datetime import datetime, timezone
from pathlib import Path

import yaml

_SCRIPT_DIR  = Path(__file__).parent
_CONFIG_PATH = _SCRIPT_DIR.parent / "config.yml"

with open(_CONFIG_PATH) as f:
    _CONFIG = yaml.safe_load(f)

CATALOG      = _CONFIG["catalog"]
WAREHOUSE_ID = _CONFIG.get("warehouse_id", "auto")
PROFILE      = _CONFIG.get("databricks_profile", "DEFAULT")
SQL_TIMEOUT  = _CONFIG.get("sql_timeout", 300)

AGENT_NAME    = "ApprovalWorkflow"
ACTIONS_TABLE = f"{CATALOG}.gold.agg_retention_actions"
HEALTH_TABLE  = f"{CATALOG}.gold.agg_company_health_score"


# ── Databricks SDK helpers ─────────────────────────────────────────────────────

def _get_client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(profile=PROFILE if PROFILE != "DEFAULT" else None)


def _get_warehouse_id(client) -> str:
    if WAREHOUSE_ID and WAREHOUSE_ID != "auto":
        return WAREHOUSE_ID
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("Aucun SQL Warehouse disponible.")
    running = [w for w in warehouses if "RUNNING" in str(w.state)]
    chosen = running[0] if running else warehouses[0]
    return chosen.id


def _sql_to_df(client, warehouse_id: str, query: str):
    """Exécute une requête SQL et retourne un DataFrame pandas."""
    import pandas as pd
    from databricks.sdk.service.sql import StatementState

    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=query,
        wait_timeout=f"{SQL_TIMEOUT}s",
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = client.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error
        raise RuntimeError(f"SQL failed: {err.message if err else 'unknown'}")

    schema    = resp.manifest.schema.columns
    col_names = [c.name for c in schema]
    rows      = resp.result.data_array or []
    return pd.DataFrame(rows, columns=col_names)


def _execute_sql(client, warehouse_id: str, statement: str) -> None:
    """Exécute un statement SQL sans retour (UPDATE...)."""
    from databricks.sdk.service.sql import StatementState

    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout=f"{SQL_TIMEOUT}s",
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = client.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error
        raise RuntimeError(f"SQL failed: {err.message if err else 'unknown'}")


# ── Requêtes métier ────────────────────────────────────────────────────────────

def get_pending_actions(client, warehouse_id: str,
                        agent_id: str = None,
                        tier: str = "All",
                        limit: int = 20) -> list[dict]:
    """
    Récupère les actions en attente de validation, enrichies du contexte entreprise.
    Triées par churn_probability DESC (les plus urgentes en premier).
    """
    agent_filter = f"AND a.agent_id = '{agent_id}'" if agent_id else ""
    tier_filter  = f"AND h.risk_tier = '{tier}'"    if tier != "All" else ""

    query = f"""
    SELECT
        r.action_id,
        r.company_id,
        r.contact_id,
        r.agent_id,
        r.action_type,
        r.channel,
        r.status,
        r.generated_content,
        r.created_at,
        r.created_by_agent,
        dc.company_name,
        dc.industry,
        dc.company_size,
        h.mrr_current,
        h.health_score,
        h.risk_tier,
        h.days_to_renewal,
        p.churn_probability,
        p.top_factors,
        c.contact_name,
        c.role AS contact_role,
        c.email AS contact_email,
        ag.agent_name,
        ag.region AS agent_region
    FROM {ACTIONS_TABLE} r
    JOIN {CATALOG}.silver.dim_companies dc ON r.company_id = dc.company_id
    JOIN {HEALTH_TABLE} h ON r.company_id = h.company_id
    LEFT JOIN (
        SELECT company_id, churn_probability, top_factors
        FROM {CATALOG}.gold.agg_churn_predictions
        WHERE prediction_date = (SELECT MAX(prediction_date)
                                 FROM {CATALOG}.gold.agg_churn_predictions)
    ) p ON r.company_id = p.company_id
    LEFT JOIN {CATALOG}.silver.dim_contacts c ON r.contact_id = c.contact_id
    LEFT JOIN {CATALOG}.silver.dim_cs_agents ag ON r.agent_id = ag.agent_id
    WHERE r.status = 'pending'
      {agent_filter}
      {tier_filter}
    ORDER BY p.churn_probability DESC NULLS LAST, h.mrr_current DESC
    LIMIT {limit}
    """
    df = _sql_to_df(client, warehouse_id, query)
    return df.to_dict(orient="records") if not df.empty else []


def approve_action(client, warehouse_id: str,
                   action_id: str, approved_by: str) -> None:
    """Met à jour le statut → 'approved' avec timestamp et validateur."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _execute_sql(client, warehouse_id, f"""
        UPDATE {ACTIONS_TABLE}
        SET    status      = 'approved',
               approved_at = CAST('{now}' AS TIMESTAMP)
        WHERE  action_id   = '{action_id}'
    """)


def reject_action(client, warehouse_id: str,
                  action_id: str, reason: str) -> None:
    """Met à jour le statut → 'rejected' avec le motif dans generated_content."""
    def esc(s):
        return str(s).replace("'", "''")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rejection_note = f"[REJETÉ le {now}]\nMotif : {reason}"
    _execute_sql(client, warehouse_id, f"""
        UPDATE {ACTIONS_TABLE}
        SET    status           = 'rejected',
               generated_content = '{esc(rejection_note)}',
               approved_at      = CAST('{now}' AS TIMESTAMP)
        WHERE  action_id        = '{action_id}'
    """)


def update_outcome(client, warehouse_id: str,
                   action_id: str, outcome: str) -> None:
    """
    Met à jour le résultat final d'une action (saved / churned).
    Appelée après l'exécution de l'action pour fermer la boucle de feedback.
    """
    if outcome not in ("saved", "churned"):
        raise ValueError(f"outcome invalide : '{outcome}'. Attendu: saved | churned")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _execute_sql(client, warehouse_id, f"""
        UPDATE {ACTIONS_TABLE}
        SET    outcome     = '{outcome}',
               executed_at = CAST('{now}' AS TIMESTAMP),
               status      = 'completed'
        WHERE  action_id   = '{action_id}'
    """)


def get_dashboard_stats(client, warehouse_id: str) -> dict:
    """Statistiques globales pour le tableau de bord."""
    df = _sql_to_df(client, warehouse_id, f"""
        SELECT
            COUNT(*)                                        AS total,
            SUM(CASE WHEN status = 'pending'  THEN 1 END)  AS pending,
            SUM(CASE WHEN status = 'approved' THEN 1 END)  AS approved,
            SUM(CASE WHEN status = 'rejected' THEN 1 END)  AS rejected,
            SUM(CASE WHEN status = 'completed'
                      AND outcome = 'saved'   THEN 1 END)  AS saved,
            SUM(CASE WHEN status = 'completed'
                      AND outcome = 'churned' THEN 1 END)  AS churned,
            SUM(CASE WHEN action_type = 'email'      THEN 1 END) AS n_email,
            SUM(CASE WHEN action_type = 'call'       THEN 1 END) AS n_call,
            SUM(CASE WHEN action_type = 'discount'   THEN 1 END) AS n_discount,
            SUM(CASE WHEN action_type = 'escalation' THEN 1 END) AS n_escalation
        FROM {ACTIONS_TABLE}
    """)
    if df.empty:
        return {}
    row = df.iloc[0].to_dict()
    return {k: (int(v) if v is not None else 0) for k, v in row.items()}


# ── Affichage CLI ──────────────────────────────────────────────────────────────

RISK_COLORS = {
    "High":   "\033[91m",   # Rouge
    "Medium": "\033[93m",   # Jaune
    "Low":    "\033[92m",   # Vert
}
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
RED    = "\033[91m"


def _color(text: str, code: str) -> str:
    return f"{code}{text}{RESET}"


def print_banner(stats: dict) -> None:
    print("\n" + "=" * 70)
    print(_color(f"  NovaCRM — {AGENT_NAME}", BOLD))
    print(f"  Tableau de bord Rétention Client")
    print("=" * 70)

    if stats:
        total   = stats.get("total", 0)
        pending = stats.get("pending", 0)
        saved   = stats.get("saved", 0)
        churned = stats.get("churned", 0)
        save_rate = f"{saved/(saved+churned)*100:.0f}%" if (saved + churned) > 0 else "N/A"

        print(f"\n  Actions totales : {total}  |  "
              f"{_color(f'En attente: {pending}', YELLOW)}  |  "
              f"Approuvées: {stats.get('approved',0)}  |  "
              f"Rejetées: {stats.get('rejected',0)}")
        print(f"  Complétées → {_color(f'Sauvées: {saved}', GREEN)}  "
              f"{_color(f'Churnées: {churned}', RED)}  |  "
              f"Taux de sauvegarde : {_color(save_rate, BOLD)}")
        print(f"\n  Par type → Email:{stats.get('n_email',0)}  "
              f"Appel:{stats.get('n_call',0)}  "
              f"Discount:{stats.get('n_discount',0)}  "
              f"Escalade:{stats.get('n_escalation',0)}")
    print()


def print_action_card(action: dict, index: int, total: int) -> None:
    """Affiche la fiche complète d'une action à valider."""
    tier      = action.get("risk_tier", "")
    tier_col  = RISK_COLORS.get(tier, "")
    prob      = action.get("churn_probability")
    mrr       = action.get("mrr_current")
    try:
        prob_str = f"{float(prob):.1%}" if prob is not None else "N/A"
    except (ValueError, TypeError):
        prob_str = "N/A"
    try:
        mrr_str = f"{float(mrr):,.0f} EUR/mois" if mrr is not None else "N/A"
    except (ValueError, TypeError):
        mrr_str = "N/A"

    renewal = action.get("days_to_renewal", "?")
    atype   = action.get("action_type", "").upper()

    print("\n" + "─" * 70)
    print(f"  {_color(BOLD, BOLD)}ACTION {index}/{total}{RESET}  "
          f"[{_color(tier, tier_col)}]  "
          f"Churn: {_color(prob_str, BOLD)}  |  "
          f"MRR: {_color(mrr_str, CYAN)}")
    print("─" * 70)
    print(f"  Entreprise : {_color(action.get('company_name','?'), BOLD)} "
          f"({action.get('industry','?')}, {action.get('company_size','?')})")
    print(f"  Contact    : {action.get('contact_name','N/A')} "
          f"— {action.get('contact_role','?')} "
          f"({action.get('contact_email','?')})")
    print(f"  Agent CS   : {action.get('agent_name','N/A')} "
          f"({action.get('agent_region','?')})")
    print(f"  Renouvellement dans : {renewal} jours  |  "
          f"Health score : {action.get('health_score','?')}/100")

    # Top SHAP factors
    top_factors = action.get("top_factors")
    if top_factors:
        try:
            factors = json.loads(top_factors) if isinstance(top_factors, str) else top_factors
            if isinstance(factors, list) and factors:
                labels = [f.get("feature", str(f)) if isinstance(f, dict) else str(f)
                          for f in factors[:3]]
                print(f"  Top signaux : {', '.join(labels)}")
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

    print(f"\n  {_color(f'TYPE D\'ACTION : {atype}', BOLD)} "
          f"(canal: {action.get('channel','?')})")
    print(f"  Généré par  : {action.get('created_by_agent','?')} "
          f"le {str(action.get('created_at','?'))[:16]}")

    # Contenu généré (avec retour à la ligne)
    content = action.get("generated_content", "")
    if content:
        print(f"\n{'─' * 70}")
        print(_color("  CONTENU GÉNÉRÉ :", BOLD))
        print("─" * 70)
        for line in content.split("\n"):
            wrapped = textwrap.wrap(line, width=66) or [""]
            for wline in wrapped:
                print(f"  {wline}")
    print()


def prompt_decision(action: dict) -> tuple[str, str]:
    """
    Demande la décision à l'agent CS.
    Retourne (decision, motif_ou_instructions).
    """
    print(_color("  Votre décision :", BOLD))
    print("  [A] Approuver     — envoyer l'action telle quelle")
    print("  [R] Rejeter       — abandonner l'action")
    print("  [S] Suivant       — passer à la prochaine action sans décision")
    print("  [Q] Quitter       — arrêter la session de validation")
    print()

    while True:
        try:
            choice = input("  Choix [A/R/S/Q] : ").strip().upper()
        except (EOFError, KeyboardInterrupt):
            return "Q", ""

        if choice == "A":
            try:
                approved_by = input("  Validé par (votre identifiant) : ").strip() or "cs_agent"
            except (EOFError, KeyboardInterrupt):
                approved_by = "cs_agent"
            return "A", approved_by

        elif choice == "R":
            try:
                reason = input("  Motif du rejet (obligatoire) : ").strip()
            except (EOFError, KeyboardInterrupt):
                reason = "rejeté sans motif"
            if not reason:
                print("  ⚠ Le motif est obligatoire. Réessayez.")
                continue
            return "R", reason

        elif choice == "S":
            return "S", ""

        elif choice == "Q":
            return "Q", ""

        else:
            print(f"  Choix invalide : '{choice}'. Entrez A, R, S ou Q.")


# ── Session interactive ────────────────────────────────────────────────────────

def run_interactive_session(client, warehouse_id: str, actions: list) -> dict:
    """
    Lance la session de validation interactive.
    Retourne un résumé {approved, rejected, skipped}.
    """
    counts = {"approved": 0, "rejected": 0, "skipped": 0}
    total  = len(actions)

    for i, action in enumerate(actions, 1):
        print_action_card(action, i, total)
        decision, extra = prompt_decision(action)

        if decision == "Q":
            print(f"\n  Session interrompue après {i-1} actions.")
            break

        elif decision == "S":
            counts["skipped"] += 1
            print(f"  → Action {action['action_id'][:8]}... ignorée.\n")

        elif decision == "A":
            try:
                approve_action(client, warehouse_id, action["action_id"], approved_by=extra)
                counts["approved"] += 1
                print(_color(f"  ✓ Action approuvée (validé par : {extra})\n", GREEN))
            except Exception as e:
                print(_color(f"  ✗ Erreur lors de l'approbation : {e}\n", RED))

        elif decision == "R":
            try:
                reject_action(client, warehouse_id, action["action_id"], reason=extra)
                counts["rejected"] += 1
                print(_color(f"  ✓ Action rejetée. Motif enregistré.\n", YELLOW))
            except Exception as e:
                print(_color(f"  ✗ Erreur lors du rejet : {e}\n", RED))

    return counts


def run_batch_mode(client, warehouse_id: str, actions: list,
                   auto_approve: bool = True) -> dict:
    """Mode non-interactif pour les tests : approuve ou rejette tout."""
    counts = {"approved": 0, "rejected": 0, "skipped": 0}
    decision_label = "auto-approved" if auto_approve else "auto-rejected"

    for action in actions:
        aid = action["action_id"]
        cid = action.get("company_id", "?")
        try:
            if auto_approve:
                approve_action(client, warehouse_id, aid, approved_by="batch_auto")
                counts["approved"] += 1
                print(f"  ✓ {cid} → approuvée")
            else:
                reject_action(client, warehouse_id, aid, reason="rejet automatique (batch test)")
                counts["rejected"] += 1
                print(f"  ✗ {cid} → rejetée")
        except Exception as e:
            print(f"  ✗ {cid} → ERREUR: {e}")
            counts["skipped"] += 1

    return counts


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=f"NovaCRM — {AGENT_NAME}")
    parser.add_argument("--agent-id",        default=None,  help="Filtrer par agent CS")
    parser.add_argument("--tier",            default="All",
                        choices=["High","Medium","All"],
                        help="Filtrer par risk_tier")
    parser.add_argument("--limit",           type=int, default=20,
                        help="Nombre max d'actions à présenter")
    parser.add_argument("--non-interactive", action="store_true",
                        help="Mode batch : approuve tout automatiquement")
    parser.add_argument("--reject-all",      action="store_true",
                        help="Mode batch : rejette tout automatiquement")

    # Sous-commande pour mettre à jour un outcome après exécution
    parser.add_argument("--update-outcome",  default=None,
                        help="action_id dont il faut mettre à jour l'outcome")
    parser.add_argument("--outcome",         default=None,
                        choices=["saved", "churned"],
                        help="Résultat final (saved / churned)")
    args = parser.parse_args()

    print("=" * 70)
    print(_color(f" NovaCRM — {AGENT_NAME}", BOLD))
    print(f" Human-in-the-Loop Validation")
    print("=" * 70)

    client       = _get_client()
    warehouse_id = _get_warehouse_id(client)

    # ── Mode update outcome ────────────────────────────────────────────────────
    if args.update_outcome:
        if not args.outcome:
            print("  ✗ --outcome (saved/churned) est requis avec --update-outcome")
            sys.exit(1)
        print(f"\n  Mise à jour outcome : action {args.update_outcome[:8]}... → {args.outcome}")
        update_outcome(client, warehouse_id, args.update_outcome, args.outcome)
        print(_color(f"  ✓ Outcome mis à jour : {args.outcome}", GREEN))
        sys.exit(0)

    # ── Tableau de bord global ─────────────────────────────────────────────────
    try:
        stats = get_dashboard_stats(client, warehouse_id)
        print_banner(stats)
    except Exception as e:
        print(f"  ⚠ Impossible de charger les stats : {e}")
        print()

    # ── Chargement des actions pending ────────────────────────────────────────
    print(f"  Chargement des actions en attente"
          f"{f' — agent {args.agent_id}' if args.agent_id else ''}"
          f"{f' — tier {args.tier}' if args.tier != 'All' else ''}...")

    try:
        actions = get_pending_actions(
            client, warehouse_id,
            agent_id=args.agent_id,
            tier=args.tier,
            limit=args.limit,
        )
    except Exception as e:
        print(f"  ✗ Erreur chargement des actions : {e}")
        sys.exit(1)

    if not actions:
        print(_color("\n  ✓ Aucune action en attente de validation.", GREEN))
        print("  Le portefeuille est à jour.")
        sys.exit(0)

    print(_color(f"\n  {len(actions)} action(s) en attente de validation.\n", YELLOW))

    # ── Exécution ──────────────────────────────────────────────────────────────
    if args.non_interactive or args.reject_all:
        auto_approve = not args.reject_all
        mode_label   = "APPROBATION AUTOMATIQUE" if auto_approve else "REJET AUTOMATIQUE"
        print(f"  Mode : {_color(mode_label, BOLD)}\n")
        counts = run_batch_mode(client, warehouse_id, actions, auto_approve=auto_approve)
    else:
        print("  Utilisez [A]pprouver / [R]ejeter / [S]uivant / [Q]uitter\n")
        counts = run_interactive_session(client, warehouse_id, actions)

    # ── Résumé de session ──────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print(_color("  Résumé de session", BOLD))
    print("=" * 70)
    print(f"  {_color(f\"Approuvées : {counts['approved']}\", GREEN)}  |  "
          f"{_color(f\"Rejetées : {counts['rejected']}\", YELLOW)}  |  "
          f"Ignorées : {counts['skipped']}")

    if counts["approved"] > 0:
        print(f"\n  Étape suivante :")
        print(f"  → Exécuter les actions approuvées (envoi email, création tâche CRM...)")
        print(f"  → Puis mettre à jour les outcomes :")
        print(f"    python 06_agents/03_approval_workflow.py \\")
        print(f"           --update-outcome <action_id> --outcome saved|churned")
    print("=" * 70)


if __name__ == "__main__":
    main()
