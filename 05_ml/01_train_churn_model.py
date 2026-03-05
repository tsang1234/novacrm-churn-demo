"""
05_ml / 01_train_churn_model.py
================================
Entraîne un modèle LightGBM de détection de churn à partir de la feature table
gold.feature_store_churn et loggue tout dans Databricks MLflow.

Exécution :
    cd novacrm_demo_pipeline
    pip install lightgbm scikit-learn shap mlflow databricks-sdk pyyaml matplotlib
    python 05_ml/01_train_churn_model.py

Architecture :
    - Lecture des features via Databricks SQL Warehouse (pas de cluster requis)
    - Pré-traitement : encodage billing_cycle, détection features zero-variance
    - Validation croisée 5-fold stratifiée (rapport par fold)
    - Entraînement final sur tout le dataset + log MLflow
    - Enregistrement dans Unity Catalog Model Registry
    - Artefacts loggués : feature importance, courbe ROC, rapport classification
"""

import io
import json
import os
import sys
import time
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml

warnings.filterwarnings("ignore", category=UserWarning)

# ── Résolution du config.yml (depuis n'importe quel répertoire courant) ────────
try:
    _SCRIPT_DIR = Path(__file__).parent
except NameError:
    _SCRIPT_DIR = Path("/Workspace/Shared/novacrm/05_ml")
_CONFIG_PATH = _SCRIPT_DIR.parent / "config.yml"

with open(_CONFIG_PATH) as f:
    _CONFIG = yaml.safe_load(f)

CATALOG          = _CONFIG["catalog"]
WAREHOUSE_ID     = _CONFIG.get("warehouse_id", "auto")
PROFILE          = _CONFIG.get("databricks_profile", "DEFAULT")
SQL_TIMEOUT      = _CONFIG.get("sql_timeout", 300)

FEATURE_TABLE    = f"{CATALOG}.gold.feature_store_churn"
EXPERIMENT_NAME  = f"/Shared/novacrm/{CATALOG}_churn_experiment"
# Unity Catalog registry : catalog.schema.model_name
UC_MODEL_NAME    = f"{CATALOG}.ml.novacrm_churn_model"

# ── Features ────────────────────────────────────────────────────────────────────
NUMERIC_FEATURES = [
    # Usage
    "dau_last_30d", "dau_last_90d",
    "usage_decline_pct_30d", "usage_decline_pct_90d",
    "distinct_features_used_30d", "feature_adoption_rate",
    "days_since_last_login", "active_users_ratio",
    # Support
    "ticket_count_30d", "ticket_count_90d", "ticket_velocity_14d",
    "p1_ticket_count_30d", "avg_ticket_sentiment_30d", "avg_ticket_sentiment_90d",
    "sentiment_trend", "open_ticket_count", "escalation_count_90d", "avg_ttr_hours_30d",
    # NPS
    "nps_latest", "nps_previous", "nps_delta_vs_prev", "is_detractor",
    # Contract
    "mrr_current", "mrr_trend_90d", "days_to_renewal", "contract_duration_days",
    "has_downgraded", "upgrade_count",
    # General
    "company_size_encoded", "customer_tenure_days", "num_contacts", "num_decision_makers",
]
CATEGORICAL_FEATURES = ["billing_cycle"]
ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET_COL   = "is_churned"
ID_COL       = "company_id"


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
    chosen = running[0] if running else warehouses[0]
    print(f"  Warehouse : {chosen.name} (id={chosen.id})")
    return chosen.id


def _sql_to_df(client, warehouse_id: str, query: str) -> pd.DataFrame:
    """Exécute une requête SQL et retourne un DataFrame pandas."""
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

    schema = resp.manifest.schema.columns
    col_names = [c.name for c in schema]
    rows = resp.result.data_array or []
    return pd.DataFrame(rows, columns=col_names)


# ── Chargement des données ─────────────────────────────────────────────────────

def load_features() -> pd.DataFrame:
    """Lit la feature table depuis Databricks SQL Warehouse."""
    print("\n[1/6] Chargement des features depuis Databricks...")
    client = _get_client()
    wh_id  = _get_warehouse_id(client)

    cols = ", ".join([ID_COL] + ALL_FEATURES + [TARGET_COL])
    df   = _sql_to_df(client, wh_id, f"SELECT {cols} FROM {FEATURE_TABLE}")

    # Cast types
    for col in NUMERIC_FEATURES + [TARGET_COL]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    print(f"  {len(df)} lignes chargées | {df[TARGET_COL].sum():.0f} churned "
          f"({df[TARGET_COL].mean()*100:.1f}%)")
    return df


# ── Pré-traitement ─────────────────────────────────────────────────────────────

def preprocess(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Encode les catégorielles, détecte les features zero-variance, retourne X préparé.
    Retourne (df_processed, all_feature_cols, zero_variance_cols).
    """
    print("\n[2/6] Pré-traitement...")

    df = df.copy()

    # Encodage billing_cycle → 0/1
    df["billing_cycle_annual"] = (df["billing_cycle"] == "annual").astype(int)
    df.drop(columns=["billing_cycle"], inplace=True)

    feature_cols = NUMERIC_FEATURES + ["billing_cycle_annual"]

    # Détection zero-variance
    stds = df[feature_cols].std()
    zero_var = stds[stds == 0].index.tolist()
    if zero_var:
        print(f"  ⚠  {len(zero_var)} features zero-variance détectées (toutes à 0) :")
        print(f"     {zero_var[:8]}{'...' if len(zero_var) > 8 else ''}")
        print("     → Conservées pour la prod (seront actives sur des données récentes)")

    # Stats cibles
    pos  = int(df[TARGET_COL].sum())
    neg  = len(df) - pos
    ratio = neg / max(pos, 1)
    print(f"  Classe 0 (no churn) : {neg} | Classe 1 (churn) : {pos} | ratio : {ratio:.1f}:1")

    return df, feature_cols, zero_var


# ── Validation croisée ─────────────────────────────────────────────────────────

def cross_validate(df: pd.DataFrame, feature_cols: list[str]) -> dict:
    """
    Validation croisée 5-fold stratifiée.
    Retourne les métriques moyennes et par fold.
    """
    import lightgbm as lgb
    from sklearn.model_selection import StratifiedKFold
    from sklearn.metrics import (
        roc_auc_score, average_precision_score,
        f1_score, precision_score, recall_score,
    )

    print("\n[3/6] Validation croisée (5-fold stratifiée)...")

    X = df[feature_cols].values
    y = df[TARGET_COL].values
    pos = y.sum()
    scale_pos_weight = (len(y) - pos) / max(pos, 1)

    params = {
        "objective":        "binary",
        "metric":           ["auc", "binary_logloss"],
        "n_estimators":     300,
        "learning_rate":    0.03,
        "num_leaves":       15,
        "min_child_samples": 5,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq":     5,
        "scale_pos_weight": scale_pos_weight,
        "random_state":     42,
        "verbose":          -1,
        "n_jobs":           -1,
    }

    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    fold_metrics = []

    for fold, (train_idx, val_idx) in enumerate(skf.split(X, y), 1):
        X_tr, X_val = X[train_idx], X[val_idx]
        y_tr, y_val = y[train_idx], y[val_idx]

        model = lgb.LGBMClassifier(**params)
        model.fit(
            X_tr, y_tr,
            eval_set=[(X_val, y_val)],
            callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(0)],
        )

        proba = model.predict_proba(X_val)[:, 1]
        pred  = (proba >= 0.4).astype(int)  # seuil 0.4 pour données déséquilibrées

        m = {
            "fold":       fold,
            "val_size":   len(y_val),
            "positives":  int(y_val.sum()),
            "roc_auc":    roc_auc_score(y_val, proba) if y_val.sum() > 0 else 0.5,
            "pr_auc":     average_precision_score(y_val, proba) if y_val.sum() > 0 else 0.0,
            "f1":         f1_score(y_val, pred, zero_division=0),
            "precision":  precision_score(y_val, pred, zero_division=0),
            "recall":     recall_score(y_val, pred, zero_division=0),
        }
        fold_metrics.append(m)
        print(f"  Fold {fold} | roc_auc={m['roc_auc']:.3f} | pr_auc={m['pr_auc']:.3f} "
              f"| f1={m['f1']:.3f} | recall={m['recall']:.3f} | +={m['positives']}/{m['val_size']}")

    avg = {
        k: float(np.mean([f[k] for f in fold_metrics]))
        for k in ["roc_auc", "pr_auc", "f1", "precision", "recall"]
    }
    print(f"\n  Moyenne CV → roc_auc={avg['roc_auc']:.3f} | pr_auc={avg['pr_auc']:.3f} "
          f"| f1={avg['f1']:.3f} | recall={avg['recall']:.3f}")

    return {"fold_metrics": fold_metrics, "avg_metrics": avg, "params": params}


# ── Entraînement final ─────────────────────────────────────────────────────────

def train_final(
    df: pd.DataFrame,
    feature_cols: list[str],
    cv_results: dict,
) -> tuple:
    """
    Entraîne le modèle final sur tout le dataset et loggue dans MLflow.
    Retourne (model, run_id, feature_importance_df).
    """
    import lightgbm as lgb
    import mlflow
    import mlflow.lightgbm
    from sklearn.metrics import roc_auc_score, average_precision_score

    print("\n[4/6] Entraînement final + logging MLflow...")

    # Configuration MLflow avec Databricks tracking
    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(EXPERIMENT_NAME)

    X = df[feature_cols].values
    y = df[TARGET_COL].values
    pos = y.sum()
    scale_pos_weight = (len(y) - pos) / max(pos, 1)

    params = cv_results["params"]
    avg    = cv_results["avg_metrics"]

    with mlflow.start_run(run_name=f"lgbm_churn_{CATALOG}") as run:
        run_id = run.info.run_id

        # Tags descriptifs
        mlflow.set_tags({
            "model_type":   "LightGBM",
            "dataset":      FEATURE_TABLE,
            "use_case":     "churn_detection",
            "team":         "data_science",
            "catalog":      CATALOG,
        })

        # Paramètres du modèle
        mlflow.log_params(params)
        mlflow.log_param("n_features",       len(feature_cols))
        mlflow.log_param("n_samples",        len(df))
        mlflow.log_param("n_positives",      int(pos))
        mlflow.log_param("imbalance_ratio",  round(float(scale_pos_weight), 2))
        mlflow.log_param("prediction_threshold", 0.4)

        # Métriques CV (préfixées cv_)
        for k, v in avg.items():
            mlflow.log_metric(f"cv_{k}", round(v, 4))
        for fold in cv_results["fold_metrics"]:
            mlflow.log_metric("fold_roc_auc", fold["roc_auc"], step=fold["fold"])

        # Entraînement final
        model = lgb.LGBMClassifier(**params)
        model.fit(X, y, callbacks=[lgb.log_evaluation(0)])

        # Métriques full-train (référence)
        proba_train = model.predict_proba(X)[:, 1]
        mlflow.log_metric("train_roc_auc", round(roc_auc_score(y, proba_train), 4))
        mlflow.log_metric("train_pr_auc",  round(average_precision_score(y, proba_train), 4))

        # ── Artefacts ──────────────────────────────────────────────────────────

        # 1. Feature importance
        fi_df = pd.DataFrame({
            "feature":   feature_cols,
            "importance_gain":  model.booster_.feature_importance(importance_type="gain"),
            "importance_split": model.booster_.feature_importance(importance_type="split"),
        }).sort_values("importance_gain", ascending=False)

        fig, ax = plt.subplots(figsize=(10, 8))
        top20 = fi_df.head(20)
        ax.barh(top20["feature"][::-1], top20["importance_gain"][::-1], color="#1f77b4")
        ax.set_xlabel("Importance (gain)")
        ax.set_title(f"Top 20 Feature Importances — {CATALOG} Churn Model")
        plt.tight_layout()
        mlflow.log_figure(fig, "feature_importance_top20.png")
        plt.close(fig)

        # 2. Courbe précision-rappel
        from sklearn.metrics import precision_recall_curve
        precision_vals, recall_vals, _ = precision_recall_curve(y, proba_train)
        fig2, ax2 = plt.subplots(figsize=(7, 5))
        ax2.plot(recall_vals, precision_vals, lw=2, color="#d62728")
        ax2.set_xlabel("Recall")
        ax2.set_ylabel("Precision")
        ax2.set_title("Precision-Recall Curve (train)")
        ax2.axhline(y=pos/len(y), color="grey", linestyle="--", label="Baseline")
        ax2.legend()
        plt.tight_layout()
        mlflow.log_figure(fig2, "precision_recall_curve.png")
        plt.close(fig2)

        # 3. Feature importance CSV
        mlflow.log_text(fi_df.to_csv(index=False), "feature_importance.csv")

        # 4. Métriques CV par fold en JSON
        mlflow.log_text(
            json.dumps(cv_results["fold_metrics"], indent=2),
            "cv_fold_metrics.json",
        )

        # ── Enregistrement dans Unity Catalog ──────────────────────────────────
        print(f"  Enregistrement du modèle dans UC : {UC_MODEL_NAME}")
        model_info = mlflow.lightgbm.log_model(
            lgb_model=model,
            artifact_path="model",
            registered_model_name=UC_MODEL_NAME,
            input_example=pd.DataFrame([X[0]], columns=feature_cols),
            signature=mlflow.models.infer_signature(
                pd.DataFrame(X, columns=feature_cols),
                proba_train,
            ),
        )

        print(f"\n  ✓ Run ID    : {run_id}")
        print(f"  ✓ Model URI : {model_info.model_uri}")
        print(f"  ✓ cv_roc_auc: {avg['roc_auc']:.3f}")
        print(f"  ✓ cv_pr_auc : {avg['pr_auc']:.3f}")

    return model, run_id, fi_df


# ── Rapport feature importance ─────────────────────────────────────────────────

def print_feature_importance(fi_df: pd.DataFrame, top_n: int = 15) -> None:
    print(f"\n[5/6] Top {top_n} features par importance (gain) :")
    print(f"  {'Feature':<40} {'Gain':>10}  {'Split':>8}")
    print(f"  {'─'*40} {'─'*10}  {'─'*8}")
    for _, row in fi_df.head(top_n).iterrows():
        bar = "█" * int(row["importance_gain"] / fi_df["importance_gain"].max() * 20)
        print(f"  {row['feature']:<40} {row['importance_gain']:>10.1f}  {bar}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    t0 = time.time()
    print("=" * 65)
    print(" NovaCRM — Churn Model Training")
    print(f" Catalog : {CATALOG}")
    print(f" Feature table : {FEATURE_TABLE}")
    print("=" * 65)

    # 1. Chargement
    df = load_features()

    # 2. Pré-traitement
    df_processed, feature_cols, zero_var_cols = preprocess(df)

    # 3. Cross-validation
    cv_results = cross_validate(df_processed, feature_cols)

    # 4. Entraînement final + MLflow
    model, run_id, fi_df = train_final(df_processed, feature_cols, cv_results)

    # 5. Feature importance console
    print_feature_importance(fi_df)

    # 6. Instructions suivante
    print(f"\n[6/6] Terminé en {time.time()-t0:.1f}s")
    print("\n  Étape suivante :")
    print(f"    python 05_ml/02_register_model.py")
    print(f"    → Promouvra la meilleure version de {UC_MODEL_NAME} en Production")


if __name__ == "__main__":
    main()
