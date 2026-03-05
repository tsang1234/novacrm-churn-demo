"""
05_ml / 03_batch_inference.py
===============================
Inference batch : charge le modèle champion depuis le Unity Catalog Model Registry,
score toutes les entreprises et écrit les prédictions dans gold.agg_churn_predictions.
Inclut les top-5 facteurs SHAP par entreprise pour l'explicabilité (agents AI).

Exécution (après 02_register_model.py) :
    python 05_ml/03_batch_inference.py

Options :
    python 05_ml/03_batch_inference.py --threshold 0.4
    python 05_ml/03_batch_inference.py --model-version 2   (version précise)

Output : gold.agg_churn_predictions
    company_id | prediction_date | churn_probability | risk_tier
    top_factors (JSON) | model_version | model_name
"""

import argparse
import json
import sys
import time
from datetime import date, timezone, datetime
from pathlib import Path

import pandas as pd
import yaml

try:
    _SCRIPT_DIR = Path(__file__).parent
except NameError:
    _SCRIPT_DIR = Path("/Workspace/Shared/novacrm/05_ml")
_CONFIG_PATH = _SCRIPT_DIR.parent / "config.yml"

with open(_CONFIG_PATH) as f:
    _CONFIG = yaml.safe_load(f)

CATALOG         = _CONFIG["catalog"]
WAREHOUSE_ID    = _CONFIG.get("warehouse_id", "auto")
PROFILE         = _CONFIG.get("databricks_profile", "DEFAULT")
SQL_TIMEOUT     = _CONFIG.get("sql_timeout", 300)

FEATURE_TABLE   = f"{CATALOG}.gold.feature_store_churn"
OUTPUT_TABLE    = f"{CATALOG}.gold.agg_churn_predictions"
UC_MODEL_NAME   = f"{CATALOG}.ml.novacrm_churn_model"

DEFAULT_THRESHOLD = 0.40   # seuil décision churn / no-churn

NUMERIC_FEATURES = [
    "dau_last_30d", "dau_last_90d",
    "usage_decline_pct_30d", "usage_decline_pct_90d",
    "distinct_features_used_30d", "feature_adoption_rate",
    "days_since_last_login", "active_users_ratio",
    "ticket_count_30d", "ticket_count_90d", "ticket_velocity_14d",
    "p1_ticket_count_30d", "avg_ticket_sentiment_30d", "avg_ticket_sentiment_90d",
    "sentiment_trend", "open_ticket_count", "escalation_count_90d", "avg_ttr_hours_30d",
    "nps_latest", "nps_previous", "nps_delta_vs_prev", "is_detractor",
    "mrr_current", "mrr_trend_90d", "days_to_renewal", "contract_duration_days",
    "has_downgraded", "upgrade_count",
    "company_size_encoded", "customer_tenure_days", "num_contacts", "num_decision_makers",
]
CATEGORICAL_FEATURES = ["billing_cycle"]
ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
ID_COL       = "company_id"
TARGET_COL   = "is_churned"


# ── Utilitaires Databricks SDK ──────────────────────────────────────────────────

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
    chosen  = running[0] if running else warehouses[0]
    print(f"  Warehouse : {chosen.name} (id={chosen.id})")
    return chosen.id


def _sql_to_df(client, warehouse_id: str, query: str) -> pd.DataFrame:
    from databricks.sdk.service.sql import StatementState

    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=query, wait_timeout="50s",
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = client.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error
        raise RuntimeError(f"SQL failed: {err.message if err else 'unknown'}")

    col_names = [c.name for c in resp.manifest.schema.columns]
    return pd.DataFrame(resp.result.data_array or [], columns=col_names)


def _execute_sql(client, warehouse_id: str, query: str) -> None:
    """Exécute un statement SQL sans retour de données (INSERT/MERGE/CREATE)."""
    from databricks.sdk.service.sql import StatementState

    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=query, wait_timeout="50s",
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = client.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error
        raise RuntimeError(f"SQL failed: {err.message if err else 'unknown'}")


# ── Chargement du modèle ───────────────────────────────────────────────────────

def load_model(model_version: str = None):
    """
    Charge le modèle champion depuis Unity Catalog Model Registry.
    Si model_version est fourni, charge cette version précise.
    Sinon, utilise l'alias 'champion'.
    """
    import mlflow
    import mlflow.lightgbm

    print("\n[1/5] Chargement du modèle depuis UC Registry...")
    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")

    if model_version:
        model_uri = f"models:/{UC_MODEL_NAME}/{model_version}"
    else:
        model_uri = f"models:/{UC_MODEL_NAME}@champion"

    try:
        model = mlflow.lightgbm.load_model(model_uri)
        print(f"  ✓ Modèle chargé : {model_uri}")
        return model, model_uri
    except Exception as e:
        print(f"  ✗ Impossible de charger le modèle : {e}")
        print("  → Avez-vous exécuté 01_train_churn_model.py puis 02_register_model.py ?")
        sys.exit(1)


def get_model_version_from_uri(model_uri: str) -> str:
    """Extrait la version du modèle depuis l'URI MLflow."""
    import mlflow
    from mlflow.tracking import MlflowClient

    client = MlflowClient()
    try:
        version = client.get_model_version_by_alias(UC_MODEL_NAME, "champion")
        return version.version
    except Exception:
        parts = model_uri.split("/")
        return parts[-1] if parts else "unknown"


# ── Chargement des features ────────────────────────────────────────────────────

def load_features(client, warehouse_id: str) -> pd.DataFrame:
    """Charge la feature table depuis le SQL Warehouse."""
    print("\n[2/5] Chargement des features...")

    cols = ", ".join([ID_COL] + ALL_FEATURES + [TARGET_COL])
    df   = _sql_to_df(client, warehouse_id, f"SELECT {cols} FROM {FEATURE_TABLE}")

    for col in NUMERIC_FEATURES + [TARGET_COL]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    print(f"  {len(df)} entreprises chargées")
    return df


def preprocess(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Encode billing_cycle → billing_cycle_annual."""
    df = df.copy()
    df["billing_cycle_annual"] = (df["billing_cycle"] == "annual").astype(int)
    df.drop(columns=["billing_cycle"], inplace=True)
    feature_cols = NUMERIC_FEATURES + ["billing_cycle_annual"]
    return df, feature_cols


# ── SHAP explanations ──────────────────────────────────────────────────────────

def compute_top_shap_factors(model, X: pd.DataFrame, feature_cols: list[str], top_n: int = 5) -> list[str]:
    """
    Calcule les top N facteurs SHAP par entreprise.
    Retourne une liste de JSON strings (un par entreprise).
    """
    try:
        import shap
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X)

        # shap_values peut être un array 2D (LightGBM binary)
        if isinstance(shap_values, list):
            sv = shap_values[1]   # classe positive
        else:
            sv = shap_values

        result = []
        for i in range(len(X)):
            row_shap = dict(zip(feature_cols, sv[i]))
            # Top N par valeur absolue
            top_factors = sorted(row_shap.items(), key=lambda x: abs(x[1]), reverse=True)[:top_n]
            result.append(json.dumps({k: round(float(v), 4) for k, v in top_factors}))
        return result

    except ImportError:
        print("  ⚠  shap non installé → top_factors = null (pip install shap)")
        return ["{}"] * len(X)
    except Exception as e:
        print(f"  ⚠  SHAP erreur : {e} → top_factors = null")
        return ["{}"] * len(X)


# ── Prédictions ────────────────────────────────────────────────────────────────

def predict(model, df: pd.DataFrame, feature_cols: list[str], threshold: float) -> pd.DataFrame:
    """Score toutes les entreprises et construit le DataFrame de résultats."""
    print(f"\n[3/5] Calcul des prédictions (threshold={threshold})...")

    X = df[feature_cols]

    # Probabilités de churn
    proba = model.predict_proba(X.values)[:, 1]

    # Risk tier
    risk_tier = pd.cut(
        proba,
        bins=[-0.001, 0.40, 0.70, 1.001],
        labels=["Low", "Medium", "High"],
    ).astype(str)

    # SHAP top factors
    print("  Calcul des facteurs SHAP...")
    top_factors = compute_top_shap_factors(model, X, feature_cols, top_n=5)

    n_high   = (risk_tier == "High").sum()
    n_medium = (risk_tier == "Medium").sum()
    n_low    = (risk_tier == "Low").sum()
    print(f"  High Risk  : {n_high:3d} entreprises ({n_high/len(df)*100:.1f}%)")
    print(f"  Medium Risk: {n_medium:3d} entreprises ({n_medium/len(df)*100:.1f}%)")
    print(f"  Low Risk   : {n_low:3d} entreprises ({n_low/len(df)*100:.1f}%)")

    return pd.DataFrame({
        "company_id":        df[ID_COL].values,
        "prediction_date":   str(date.today()),
        "churn_probability": [round(float(p), 4) for p in proba],
        "risk_tier":         risk_tier,
        "top_factors":       top_factors,
        "model_name":        UC_MODEL_NAME,
        "predicted_churn":   (proba >= threshold).astype(int),
        # Label réel (pour évaluation offline)
        "actual_churn":      df[TARGET_COL].values,
    })


# ── Écriture dans Gold ─────────────────────────────────────────────────────────

def write_predictions(client, warehouse_id: str, preds_df: pd.DataFrame, model_version: str) -> None:
    """
    Écrit les prédictions dans gold.agg_churn_predictions via MERGE.
    Idempotent : re-run le même jour = mise à jour (pas de doublon).
    """
    print(f"\n[4/5] Écriture dans {OUTPUT_TABLE}...")

    today = str(date.today())

    # Construction du VALUES clause par batch de 100
    rows_sql = []
    for _, row in preds_df.iterrows():
        prob    = row["churn_probability"]
        tier    = row["risk_tier"]
        factors = row["top_factors"].replace("'", "''")  # escape SQL
        company = row["company_id"]
        rows_sql.append(
            f"('{company}', DATE'{today}', {prob}, '{tier}', '{factors}', "
            f"'{model_version}', '{UC_MODEL_NAME}')"
        )

    values_str = ",\n    ".join(rows_sql)

    merge_sql = f"""
MERGE INTO {OUTPUT_TABLE} AS target
USING (
  SELECT * FROM (
    VALUES
    {values_str}
  ) AS t(company_id, prediction_date, churn_probability, risk_tier, top_factors, model_version, model_name)
) AS source
ON target.company_id = source.company_id
   AND target.prediction_date = source.prediction_date
WHEN MATCHED THEN UPDATE SET
  target.churn_probability = source.churn_probability,
  target.risk_tier         = source.risk_tier,
  target.top_factors       = source.top_factors,
  target.model_version     = source.model_version,
  target.model_name        = source.model_name
WHEN NOT MATCHED THEN INSERT (
  company_id, prediction_date, churn_probability, risk_tier, top_factors, model_version, model_name
) VALUES (
  source.company_id, source.prediction_date, source.churn_probability, source.risk_tier,
  source.top_factors, source.model_version, source.model_name
)
"""
    _execute_sql(client, warehouse_id, merge_sql)

    # Vérification
    result = _sql_to_df(
        client, warehouse_id,
        f"SELECT COUNT(*) AS n, SUM(CASE WHEN risk_tier='High' THEN 1 ELSE 0 END) AS high "
        f"FROM {OUTPUT_TABLE} WHERE prediction_date = DATE'{today}'"
    )
    n    = int(result["n"].iloc[0])
    high = int(result["high"].iloc[0])
    print(f"  ✓ {n} prédictions écrites pour {today} | {high} High Risk")


# ── Résumé et validation offline ──────────────────────────────────────────────

def print_summary(preds_df: pd.DataFrame, threshold: float) -> None:
    """Affiche métriques offline si les labels réels sont disponibles."""
    print("\n[5/5] Résumé & validation offline...")

    # Distribution globale
    print(f"\n  Distribution des probabilités de churn :")
    bins = [0, 0.2, 0.4, 0.6, 0.8, 1.0]
    labels = ["0-20%", "20-40%", "40-60%", "60-80%", "80-100%"]
    preds_df["prob_bin"] = pd.cut(preds_df["churn_probability"], bins=bins, labels=labels)
    for label, cnt in preds_df["prob_bin"].value_counts().sort_index().items():
        bar = "█" * int(cnt / len(preds_df) * 40)
        print(f"    {label} : {bar} {cnt}")

    # Métriques offline (si labels disponibles)
    if "actual_churn" in preds_df.columns:
        try:
            from sklearn.metrics import roc_auc_score, average_precision_score, classification_report
            y_true  = preds_df["actual_churn"].values.astype(int)
            y_proba = preds_df["churn_probability"].values
            y_pred  = (y_proba >= threshold).astype(int)

            if y_true.sum() > 0:
                auc = roc_auc_score(y_true, y_proba)
                pr  = average_precision_score(y_true, y_proba)
                print(f"\n  Métriques offline (labels réels) :")
                print(f"    roc_auc = {auc:.3f} | pr_auc = {pr:.3f}")
                print(f"\n{classification_report(y_true, y_pred, target_names=['no_churn','churn'])}")
        except ImportError:
            pass

    # Top 10 entreprises les plus à risque
    print("\n  Top 10 entreprises les plus à risque :")
    top10 = preds_df.nlargest(10, "churn_probability")[
        ["company_id", "churn_probability", "risk_tier", "top_factors"]
    ]
    for _, row in top10.iterrows():
        factors = list(json.loads(row["top_factors"]).keys())[:3]
        print(f"    {row['company_id']}  {row['churn_probability']:.3f}  "
              f"{row['risk_tier']:<8}  top factors: {', '.join(factors)}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Batch inference churn model")
    parser.add_argument("--threshold",     type=float, default=DEFAULT_THRESHOLD,
                        help=f"Seuil de décision churn (défaut: {DEFAULT_THRESHOLD})")
    parser.add_argument("--model-version", default=None,
                        help="Version spécifique du modèle (défaut: alias champion)")
    args = parser.parse_args()

    t0 = time.time()
    print("=" * 65)
    print(" NovaCRM — Batch Churn Inference")
    print(f" Catalog  : {CATALOG}")
    print(f" Output   : {OUTPUT_TABLE}")
    print(f" Threshold: {args.threshold}")
    print("=" * 65)

    # 1. Chargement modèle
    model, model_uri = load_model(args.model_version)
    model_version    = get_model_version_from_uri(model_uri)
    print(f"  Model version: {model_version}")

    # 2. Données
    client     = _get_client()
    warehouse  = _get_warehouse_id(client)
    df         = load_features(client, warehouse)
    df_proc, feature_cols = preprocess(df)

    # 3. Prédictions + SHAP
    preds_df = predict(model, df_proc, feature_cols, args.threshold)

    # 4. Écriture Gold
    write_predictions(client, warehouse, preds_df, model_version)

    # 5. Résumé
    print_summary(preds_df, args.threshold)

    print(f"\n  ✓ Terminé en {time.time()-t0:.1f}s")
    print("\n  Étape suivante :")
    print("    python 06_agents/01_risk_detection_agent.py")
    print("    → Traitera les entreprises High Risk et générera les actions de rétention")


if __name__ == "__main__":
    main()
