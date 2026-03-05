"""
run_deploy.py
=============
Orchestrateur de déploiement NovaCRM — ML Pipeline + Agent Workflow.

Séquence :
  1. ml_pipeline    → 07_deploy/02_ml_workflow.py
  2. agent_workflow → 07_deploy/03_agent_workflow.py

Utilisation :
    python run_deploy.py                          # pipeline complet
    python run_deploy.py --step ml_pipeline       # ML seulement
    python run_deploy.py --step agent_workflow    # Agents seulement
    python run_deploy.py --no-run                 # Créer jobs sans lancer
    python run_deploy.py --no-upload              # Re-run sans re-uploader
"""

import argparse
import sys
from pathlib import Path

import yaml

_SCRIPT_DIR  = Path(__file__).parent
_CONFIG_PATH = _SCRIPT_DIR / "config.yml"

with open(_CONFIG_PATH) as f:
    _CONFIG = yaml.safe_load(f)

CATALOG = _CONFIG["catalog"]


STEPS = {
    "ml_pipeline":    "07_deploy/02_ml_workflow",
    "agent_workflow": "07_deploy/03_agent_workflow",
}

STEP_ORDER = ["ml_pipeline", "agent_workflow"]


def run_step(step_name: str, no_run: bool, no_upload: bool) -> None:
    module_path = STEPS[step_name]
    print(f"\n{'#' * 65}")
    print(f"# ÉTAPE : {step_name}")
    print(f"{'#' * 65}\n")

    # Import dynamique du module de déploiement
    import importlib.util
    module_file = _SCRIPT_DIR / f"{module_path}.py"
    spec   = importlib.util.spec_from_file_location(step_name, module_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Patcher sys.argv pour passer les flags au main() du module
    original_argv = sys.argv[:]
    sys.argv = [str(module_file)]
    if no_run:
        sys.argv.append("--no-run")
    if no_upload:
        sys.argv.append("--no-upload")

    try:
        module.main()
    finally:
        sys.argv = original_argv


def main():
    parser = argparse.ArgumentParser(
        description="NovaCRM — Orchestrateur de déploiement Databricks"
    )
    parser.add_argument(
        "--step",
        choices=list(STEPS.keys()),
        default=None,
        help="Exécuter une seule étape (défaut : toutes)",
    )
    parser.add_argument(
        "--no-run",
        action="store_true",
        help="Créer les jobs Databricks sans les lancer",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Ne pas uploader les fichiers (utilise les versions workspace existantes)",
    )
    args = parser.parse_args()

    steps_to_run = [args.step] if args.step else STEP_ORDER

    print("=" * 65)
    print(" NovaCRM — Déploiement Databricks")
    print(f" Catalog  : {CATALOG}")
    print(f" Étapes   : {' → '.join(steps_to_run)}")
    if args.no_run:
        print(" Mode     : création jobs uniquement (--no-run)")
    if args.no_upload:
        print(" Upload   : désactivé (--no-upload)")
    print("=" * 65)

    for step in steps_to_run:
        try:
            run_step(step, no_run=args.no_run, no_upload=args.no_upload)
        except SystemExit as e:
            if e.code and e.code != 0:
                print(f"\n  ✗ Étape '{step}' échouée (code={e.code}). Arrêt.")
                sys.exit(e.code)
        except Exception as e:
            print(f"\n  ✗ Étape '{step}' : erreur inattendue — {e}")
            raise

    print(f"\n{'=' * 65}")
    print(f"  ✓ Déploiement terminé ({len(steps_to_run)} étape(s))")
    print("=" * 65)


if __name__ == "__main__":
    main()
