"""
05_ml / 02_register_model.py
==============================
Promeut la meilleure version du modèle LightGBM en Production dans
Unity Catalog Model Registry, ajoute les tags de gouvernance et
l'alias "champion".

Exécution (après 01_train_churn_model.py) :
    python 05_ml/02_register_model.py

Ou en spécifiant un run_id précis :
    python 05_ml/02_register_model.py --run-id abc123def456

Logique de sélection :
    - Récupère toutes les versions du modèle dans l'UC Registry
    - Filtre sur cv_roc_auc >= MIN_AUC_THRESHOLD
    - Promeut la version avec le meilleur cv_roc_auc
    - Met à jour l'alias "champion" (standard Databricks MLflow v2)
"""

import argparse
import sys
import time
from pathlib import Path

import yaml

try:
    _SCRIPT_DIR = Path(__file__).parent
except NameError:
    _SCRIPT_DIR = Path("/Workspace/Shared/novacrm/05_ml")
_CONFIG_PATH = _SCRIPT_DIR.parent / "config.yml"

with open(_CONFIG_PATH) as f:
    _CONFIG = yaml.safe_load(f)

CATALOG             = _CONFIG["catalog"]
UC_MODEL_NAME       = f"{CATALOG}.ml.novacrm_churn_model"
MIN_AUC_THRESHOLD   = 0.60   # seuil minimal pour aller en production
EXPERIMENT_NAME     = f"/Shared/novacrm/{CATALOG}_churn_experiment"


# ── MLflow client ──────────────────────────────────────────────────────────────

def _get_mlflow_client():
    import mlflow
    from mlflow.tracking import MlflowClient
    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    return MlflowClient()


# ── Sélection du meilleur run ──────────────────────────────────────────────────

def find_best_run(client, run_id_override: str = None) -> dict:
    """
    Trouve le meilleur run MLflow pour le modèle de churn.
    Si run_id_override est fourni, utilise ce run directement.
    Sinon, cherche dans l'expérience le run avec le meilleur cv_roc_auc.
    """
    import mlflow

    mlflow.set_tracking_uri("databricks")
    print("\n[1/4] Recherche du meilleur run MLflow...")

    if run_id_override:
        run = client.get_run(run_id_override)
        auc = run.data.metrics.get("cv_roc_auc", 0.0)
        print(f"  Run spécifié : {run_id_override} | cv_roc_auc={auc:.3f}")
        return {"run_id": run_id_override, "cv_roc_auc": auc, "run": run}

    # Chercher l'expérience
    try:
        experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
        if experiment is None:
            raise ValueError(f"Expérience '{EXPERIMENT_NAME}' introuvable.")
    except Exception as e:
        print(f"  ✗ {e}")
        print("  → Avez-vous exécuté 01_train_churn_model.py ?")
        sys.exit(1)

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string="attributes.status = 'FINISHED'",
        order_by=["metrics.cv_roc_auc DESC"],
        max_results=20,
    )

    if not runs:
        print("  ✗ Aucun run terminé trouvé.")
        sys.exit(1)

    # Filtrer par seuil AUC
    valid_runs = [r for r in runs if r.data.metrics.get("cv_roc_auc", 0) >= MIN_AUC_THRESHOLD]
    if not valid_runs:
        best_auc = runs[0].data.metrics.get("cv_roc_auc", 0)
        print(f"  ✗ Aucun run avec cv_roc_auc ≥ {MIN_AUC_THRESHOLD} "
              f"(meilleur trouvé : {best_auc:.3f})")
        print("  → Vérifiez les données ou abaissez MIN_AUC_THRESHOLD.")
        sys.exit(1)

    best = valid_runs[0]
    auc  = best.data.metrics["cv_roc_auc"]
    pr   = best.data.metrics.get("cv_pr_auc", 0)
    f1   = best.data.metrics.get("cv_f1", 0)
    print(f"  Meilleur run : {best.info.run_id}")
    print(f"  cv_roc_auc={auc:.3f} | cv_pr_auc={pr:.3f} | cv_f1={f1:.3f}")
    print(f"  Parmi {len(valid_runs)} runs valides / {len(runs)} runs totaux")

    return {"run_id": best.info.run_id, "cv_roc_auc": auc, "run": best}


# ── Récupération de la version correspondant au run ────────────────────────────

def get_model_version_from_run(client, run_id: str) -> str:
    """Trouve la version du modèle UC associée au run_id."""
    print(f"\n[2/4] Récupération de la version du modèle pour run={run_id[:8]}...")

    versions = client.search_model_versions(f"name='{UC_MODEL_NAME}'")
    matching = [v for v in versions if v.run_id == run_id]

    if not matching:
        print(f"  ✗ Aucune version trouvée pour run_id={run_id}")
        print(f"    Versions disponibles : {[v.version for v in versions]}")
        sys.exit(1)

    version = matching[0].version
    print(f"  Version trouvée : v{version} (run={run_id[:8]}...)")
    return version


# ── Promotion en Production (alias "champion") ─────────────────────────────────

def set_champion_alias(client, version: str, best_run: dict) -> None:
    """
    Dans MLflow >= 2.x avec UC, on utilise les aliases plutôt que les stages.
    Alias "champion" = version en production active.
    Alias "challenger" = version précédente (conservée pour rollback).
    """
    print(f"\n[3/4] Promotion de la version v{version} avec l'alias 'champion'...")

    # Vérifier s'il y a un champion actuel → le rétrograder en challenger
    try:
        current_champion = client.get_model_version_by_alias(UC_MODEL_NAME, "champion")
        if current_champion.version != version:
            print(f"  Champion actuel : v{current_champion.version} → devient 'challenger'")
            client.set_registered_model_alias(
                name=UC_MODEL_NAME,
                alias="challenger",
                version=current_champion.version,
            )
    except Exception:
        print("  Pas de champion actuel — première mise en production.")

    # Promouvoir la nouvelle version
    client.set_registered_model_alias(
        name=UC_MODEL_NAME,
        alias="champion",
        version=version,
    )
    print(f"  ✓ Alias 'champion' → v{version}")


# ── Tags de gouvernance ────────────────────────────────────────────────────────

def tag_model_version(client, version: str, best_run: dict) -> None:
    """Ajoute des tags de gouvernance sur la version et le modèle."""
    print(f"\n[4/4] Ajout des tags de gouvernance...")

    auc   = best_run["cv_roc_auc"]
    run   = best_run["run"]
    n_pos = run.data.params.get("n_positives", "?")
    n_tot = run.data.params.get("n_samples", "?")

    # Tags sur la version (contexte technique)
    version_tags = {
        "cv_roc_auc":     str(round(auc, 4)),
        "cv_pr_auc":      str(round(run.data.metrics.get("cv_pr_auc", 0), 4)),
        "cv_f1":          str(round(run.data.metrics.get("cv_f1", 0), 4)),
        "n_samples":      str(n_tot),
        "n_positives":    str(n_pos),
        "feature_table":  f"{CATALOG}.gold.feature_store_churn",
        "promoted_at":    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    for k, v in version_tags.items():
        client.set_model_version_tag(UC_MODEL_NAME, version, k, v)

    # Description du modèle enregistré
    client.update_registered_model(
        name=UC_MODEL_NAME,
        description=(
            f"Modèle de détection de churn client pour NovaCRM Solutions. "
            f"LightGBM binaire entraîné sur {n_tot} entreprises ({n_pos} churned). "
            f"Features : usage produit, support tickets, NPS, contrat. "
            f"cv_roc_auc={round(auc,3)}. "
            f"Données : {CATALOG}.gold.feature_store_churn."
        ),
    )

    # Description de la version
    client.update_model_version(
        name=UC_MODEL_NAME,
        version=version,
        description=f"Champion v{version} | cv_roc_auc={round(auc,3)} | run={best_run['run_id'][:8]}",
    )

    for k, v in version_tags.items():
        print(f"  {k:<20} = {v}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Promouvoir le meilleur modèle en Production")
    parser.add_argument("--run-id", default=None, help="Run MLflow ID spécifique (optionnel)")
    args = parser.parse_args()

    print("=" * 65)
    print(" NovaCRM — Model Registry Promotion")
    print(f" Modèle UC : {UC_MODEL_NAME}")
    print("=" * 65)

    client = _get_mlflow_client()

    # 1. Sélection du meilleur run
    best_run = find_best_run(client, run_id_override=args.run_id)

    # 2. Version correspondante dans l'UC Registry
    version = get_model_version_from_run(client, best_run["run_id"])

    # 3. Alias "champion"
    set_champion_alias(client, version, best_run)

    # 4. Tags de gouvernance
    tag_model_version(client, version, best_run)

    print("\n" + "=" * 65)
    print(f"  ✓ Modèle {UC_MODEL_NAME} v{version} promu en production (alias: champion)")
    print(f"\n  Étape suivante :")
    print(f"    python 05_ml/03_batch_inference.py")
    print(f"    → Scorera les 500 entreprises et écrira dans gold.agg_churn_predictions")
    print("=" * 65)


if __name__ == "__main__":
    main()
