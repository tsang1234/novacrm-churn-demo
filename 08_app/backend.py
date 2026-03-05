"""
08_app / backend.py
====================
Couche d'accès aux données — SQL Warehouse sur Unity Catalog.
Toutes les lectures/écritures passent par cette classe.
"""

import os
from contextlib import contextmanager
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config
from mlflow.deployments import get_deploy_client

CATALOG = "novacrm_demo"

ACTIONS_TABLE     = f"{CATALOG}.gold.agg_retention_actions"
PREDICTIONS_TABLE = f"{CATALOG}.gold.agg_churn_predictions"
HEALTH_TABLE      = f"{CATALOG}.gold.agg_company_health_score"
COMPANIES_TABLE   = f"{CATALOG}.silver.dim_companies"


# ── Connexion ──────────────────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    cfg = Config()
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
    )


@contextmanager
def cursor():
    conn = get_connection()
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


def _df(cur, query: str, params=None) -> pd.DataFrame:
    cur.execute(query, params)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def _exec(query: str) -> None:
    with cursor() as cur:
        cur.execute(query)


# ── KPIs ───────────────────────────────────────────────────────────────────────

def get_stats() -> dict:
    with cursor() as cur:
        df = _df(cur, f"""
            SELECT
                (SELECT COUNT(*) FROM {PREDICTIONS_TABLE}) AS total_companies,
                (SELECT COUNT(*) FROM {PREDICTIONS_TABLE} WHERE risk_tier = 'High') AS high_risk,
                (SELECT COUNT(*) FROM {ACTIONS_TABLE} WHERE status = 'pending') AS pending_actions,
                (SELECT COUNT(*) FROM {ACTIONS_TABLE} WHERE status = 'approved') AS approved_actions,
                (SELECT COUNT(*) FROM {ACTIONS_TABLE} WHERE status = 'rejected') AS rejected_actions,
                (SELECT ROUND(SUM(h.mrr), 0)
                 FROM {ACTIONS_TABLE} a
                 JOIN {HEALTH_TABLE} h ON a.company_id = h.company_id
                 WHERE a.status = 'pending') AS mrr_at_risk
        """)
    return df.iloc[0].to_dict()


# ── LLM Chat ─────────────────────────────────────────────────────────────────

LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"


def get_churn_context() -> str:
    """Build a text summary of churn data for the LLM system prompt."""
    with cursor() as cur:
        # Top 10 highest churn probability
        top10 = _df(cur, f"""
            SELECT
                dc.company_name,
                dc.sector_label AS industry,
                dc.company_size,
                h.mrr,
                h.health_score,
                h.risk_tier,
                h.days_to_renewal,
                p.churn_probability,
                p.top_factors
            FROM {PREDICTIONS_TABLE} p
            JOIN {COMPANIES_TABLE} dc ON p.company_id = dc.company_id
            JOIN {HEALTH_TABLE}    h  ON p.company_id = h.company_id
            ORDER BY p.churn_probability DESC
            LIMIT 10
        """)

        # Global stats
        stats = _df(cur, f"""
            SELECT
                COUNT(*)                                                       AS total,
                SUM(CASE WHEN p.risk_tier = 'High'   THEN 1 ELSE 0 END)       AS n_high,
                SUM(CASE WHEN p.risk_tier = 'Medium'  THEN 1 ELSE 0 END)      AS n_medium,
                SUM(CASE WHEN p.risk_tier = 'Low'    THEN 1 ELSE 0 END)       AS n_low,
                ROUND(SUM(CASE WHEN p.risk_tier = 'High' THEN h.mrr ELSE 0 END), 0) AS mrr_high_risk
            FROM {PREDICTIONS_TABLE} p
            JOIN {HEALTH_TABLE} h ON p.company_id = h.company_id
        """)

        # Pending actions summary
        actions = _df(cur, f"""
            SELECT action_type, status, COUNT(*) AS n
            FROM {ACTIONS_TABLE}
            GROUP BY action_type, status
            ORDER BY action_type, status
        """)

    # Format top 10
    lines = []
    for _, r in top10.iterrows():
        lines.append(
            f"- {r['company_name']} | {r['industry']} | {r['company_size']} | "
            f"MRR {int(r['mrr'])}€ | Health {r['health_score']}/100 | "
            f"Churn {float(r['churn_probability']):.1%} | {r['risk_tier']} | "
            f"Renouvellement J-{r['days_to_renewal']} | Factors: {r['top_factors']}"
        )

    s = stats.iloc[0]
    ctx = (
        f"=== Statistiques globales ===\n"
        f"Total entreprises : {int(s['total'])}\n"
        f"High risk : {int(s['n_high'])} | Medium : {int(s['n_medium'])} | Low : {int(s['n_low'])}\n"
        f"MRR total High risk : {int(s['mrr_high_risk'])}€\n\n"
        f"=== Top 10 entreprises par probabilité de churn ===\n"
        + "\n".join(lines)
    )

    if not actions.empty:
        action_lines = [f"- {r['action_type']} / {r['status']} : {int(r['n'])}" for _, r in actions.iterrows()]
        ctx += "\n\n=== Actions de rétention ===\n" + "\n".join(action_lines)

    return ctx


def chat_with_llm(messages: list[dict]) -> str:
    """Call the LLM via Databricks Model Serving."""
    deploy_client = get_deploy_client("databricks")
    response = deploy_client.predict(
        endpoint=LLM_ENDPOINT,
        inputs={"messages": messages, "max_tokens": 4096},
    )
    return response["choices"][0]["message"]["content"]


def get_risk_distribution() -> pd.DataFrame:
    with cursor() as cur:
        return _df(cur, f"""
            SELECT risk_tier, COUNT(*) AS n, ROUND(AVG(churn_probability)*100, 1) AS avg_prob
            FROM {PREDICTIONS_TABLE}
            GROUP BY risk_tier
            ORDER BY CASE risk_tier WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END
        """)


def get_action_distribution() -> pd.DataFrame:
    with cursor() as cur:
        return _df(cur, f"""
            SELECT action_type, status, COUNT(*) AS n
            FROM {ACTIONS_TABLE}
            GROUP BY action_type, status
            ORDER BY action_type
        """)


# ── Actions ────────────────────────────────────────────────────────────────────

def get_pending_actions() -> pd.DataFrame:
    with cursor() as cur:
        return _df(cur, f"""
            SELECT
                a.action_id,
                a.company_id,
                dc.company_name,
                dc.sector_label             AS industry,
                dc.company_size,
                h.mrr,
                h.days_to_renewal,
                h.health_score,
                h.risk_tier                 AS health_risk_tier,
                p.churn_probability,
                p.top_factors,
                a.action_type,
                a.channel,
                a.status,
                a.generated_content,
                a.created_at,
                a.created_by_agent
            FROM {ACTIONS_TABLE} a
            JOIN {COMPANIES_TABLE}  dc ON a.company_id = dc.company_id
            JOIN {HEALTH_TABLE}     h  ON a.company_id = h.company_id
            LEFT JOIN {PREDICTIONS_TABLE} p
                   ON a.company_id = p.company_id
            WHERE a.status = 'pending'
            ORDER BY h.mrr DESC, p.churn_probability DESC
        """)


def get_all_actions() -> pd.DataFrame:
    with cursor() as cur:
        return _df(cur, f"""
            SELECT
                a.action_id,
                a.company_id,
                dc.company_name,
                h.mrr,
                a.action_type,
                a.status,
                a.outcome,
                a.created_at,
                a.approved_at
            FROM {ACTIONS_TABLE} a
            JOIN {COMPANIES_TABLE} dc ON a.company_id = dc.company_id
            JOIN {HEALTH_TABLE}    h  ON a.company_id = h.company_id
            ORDER BY a.created_at DESC
        """)


def approve_action(action_id: str, notes: str = "") -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    notes_esc = notes.replace("'", "''")
    _exec(f"""
        UPDATE {ACTIONS_TABLE}
        SET status       = 'approved',
            outcome      = 'approved',
            approved_at  = CAST('{now}' AS TIMESTAMP)
            {f", generated_content = '{notes_esc}'" if notes else ""}
        WHERE action_id = '{action_id}'
    """)


def reject_action(action_id: str) -> None:
    _exec(f"""
        UPDATE {ACTIONS_TABLE}
        SET status  = 'rejected',
            outcome = 'rejected'
        WHERE action_id = '{action_id}'
    """)


# ── Prédictions ────────────────────────────────────────────────────────────────

def get_predictions() -> pd.DataFrame:
    with cursor() as cur:
        return _df(cur, f"""
            SELECT
                p.company_id,
                dc.company_name,
                dc.sector_label     AS industry,
                dc.company_size,
                h.mrr,
                h.days_to_renewal,
                h.health_score,
                p.churn_probability,
                p.risk_tier,
                p.top_factors,
                p.prediction_date
            FROM {PREDICTIONS_TABLE} p
            JOIN {COMPANIES_TABLE} dc ON p.company_id = dc.company_id
            JOIN {HEALTH_TABLE}    h  ON p.company_id = h.company_id
            ORDER BY p.churn_probability DESC
        """)
