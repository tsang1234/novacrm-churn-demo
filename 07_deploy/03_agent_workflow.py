"""
07_deploy / 03_agent_workflow.py
==================================
Upload des scripts agents et création/lancement du Databricks Job
"NovaCRM Agent Workflow".

Pipeline 2 tâches enchaînées :
  risk_detection   → 01_risk_detection_agent.py
  retention_action → 02_retention_action_agent.py (dépend de risk_detection)

Compute : Serverless + environnement pip isolé.
LLM     : Databricks Foundation Model APIs (databricks-meta-llama-3-3-70b-instruct)

Utilisation :
    python 07_deploy/03_agent_workflow.py
    python 07_deploy/03_agent_workflow.py --no-run
    python 07_deploy/03_agent_workflow.py --no-upload
"""

import sys
import time
import argparse
from pathlib import Path

import yaml

_SCRIPT_DIR   = Path(__file__).parent
_CONFIG_PATH  = _SCRIPT_DIR.parent / "config.yml"
_AGENTS_DIR   = _SCRIPT_DIR.parent / "06_agents"

with open(_CONFIG_PATH) as f:
    _CONFIG = yaml.safe_load(f)

CATALOG        = _CONFIG["catalog"]
PROFILE        = _CONFIG.get("databricks_profile", "DEFAULT")
WORKSPACE_BASE = _CONFIG.get("workspace_base", "/Shared/novacrm")
LLM_ENDPOINT   = _CONFIG.get("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")

JOB_NAME       = "NovaCRM Agent Workflow"
WORKSPACE_PATH = f"{WORKSPACE_BASE}/06_agents"

PIP_DEPS = [
    "mlflow>=2.13",
    "databricks-sdk>=0.28",
    "pyyaml",
    "pandas",
]

AGENT_SCRIPTS = [
    "01_risk_detection_agent.py",
    "02_retention_action_agent.py",
    "03_approval_workflow.py",
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def get_client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(profile=PROFILE if PROFILE != "DEFAULT" else None)


def upload_files(w, no_upload: bool) -> None:
    """Upload config.yml et 06_agents/*.py vers le workspace Databricks."""
    if no_upload:
        print("  [--no-upload] Upload ignoré.")
        return

    from databricks.sdk.service.workspace import ImportFormat

    # config.yml à la racine du workspace_base
    config_bytes = _CONFIG_PATH.read_bytes()
    dest_config  = f"{WORKSPACE_BASE}/config.yml"
    w.workspace.upload(dest_config, content=config_bytes,
                       overwrite=True, format=ImportFormat.AUTO)
    print(f"  ✓ Uploadé : {dest_config}")

    # Scripts agents
    for script_name in AGENT_SCRIPTS:
        src  = _AGENTS_DIR / script_name
        dest = f"{WORKSPACE_PATH}/{script_name}"
        w.workspace.upload(dest, content=src.read_bytes(),
                           overwrite=True, format=ImportFormat.SOURCE)
        print(f"  ✓ Uploadé : {dest}")


def find_or_create_job(w) -> int:
    """Crée le job agent ou retourne l'ID si existant."""
    for job in w.jobs.list(name=JOB_NAME):
        print(f"  Job existant trouvé : {JOB_NAME} (id={job.job_id})")
        return job.job_id

    job = w.jobs.create(
        name=JOB_NAME,
        tasks=[
            {
                "task_key": "risk_detection",
                "description": "Détection risque churn (ReAct + tool_use) → gold.agg_retention_actions status=queued",
                "spark_python_task": {
                    "python_file": f"{WORKSPACE_PATH}/01_risk_detection_agent.py",
                },
                "environment_key": "default_env",
            },
            {
                "task_key": "retention_action",
                "description": "Génération contenu rétention → gold.agg_retention_actions status=pending",
                "depends_on": [{"task_key": "risk_detection"}],
                "spark_python_task": {
                    "python_file": f"{WORKSPACE_PATH}/02_retention_action_agent.py",
                },
                "environment_key": "default_env",
            },
        ],
        environments=[
            {
                "environment_key": "default_env",
                "spec": {
                    "client": "1",
                    "dependencies": PIP_DEPS,
                },
            }
        ],
        tags={
            "project":      "novacrm-churn",
            "stage":        "agents",
            "llm_endpoint": LLM_ENDPOINT,
        },
    )
    print(f"  ✓ Job créé : {JOB_NAME} (id={job.job_id})")
    return job.job_id


def run_and_poll(w, job_id: int) -> bool:
    """Lance le job et poll jusqu'à TERMINATED. Retourne True si succès."""
    run = w.jobs.run_now(job_id=job_id)
    run_id = run.run_id
    print(f"  Run lancé (run_id={run_id})")
    print("  Polling", end="", flush=True)

    while True:
        time.sleep(15)
        run_info = w.jobs.runs.get(run_id=run_id)
        state    = run_info.state
        lc_state = state.life_cycle_state.value if state.life_cycle_state else "UNKNOWN"
        print(".", end="", flush=True)

        if lc_state == "TERMINATED":
            print()
            result = state.result_state.value if state.result_state else "UNKNOWN"
            print(f"  État final : {result}")
            return result == "SUCCESS"

        if lc_state in ("INTERNAL_ERROR", "SKIPPED"):
            print()
            print(f"  État final : {lc_state}")
            return False


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="NovaCRM — Deploy Agent Workflow")
    parser.add_argument("--no-run",    action="store_true",
                        help="Créer le job sans le lancer")
    parser.add_argument("--no-upload", action="store_true",
                        help="Ne pas uploader les fichiers (re-run job existant)")
    args = parser.parse_args()

    print("=" * 65)
    print(f" NovaCRM — {JOB_NAME}")
    print(f" Catalog        : {CATALOG}")
    print(f" LLM Endpoint   : {LLM_ENDPOINT}")
    print(f" Workspace path : {WORKSPACE_PATH}")
    print("=" * 65)

    w = get_client()

    print(f"\n[1/3] Upload des fichiers → {WORKSPACE_PATH}")
    upload_files(w, args.no_upload)

    print(f"\n[2/3] Création/vérification du job Databricks...")
    job_id = find_or_create_job(w)

    if args.no_run:
        print(f"\n  [--no-run] Job prêt (id={job_id}), non lancé.")
        print("=" * 65)
        return job_id

    print(f"\n[3/3] Lancement du job (id={job_id})...")
    success = run_and_poll(w, job_id)

    if success:
        print(f"\n{'=' * 65}")
        print(f"  ✓ {JOB_NAME} terminé avec succès")
        print(f"  → gold.agg_retention_actions remplie (status='pending')")
        print(f"\n  Étape suivante :")
        print(f"    python 06_agents/03_approval_workflow.py")
        print("=" * 65)
    else:
        print(f"\n{'=' * 65}")
        print(f"  ✗ {JOB_NAME} échoué — vérifier les logs dans Databricks UI")
        print("=" * 65)
        sys.exit(1)

    return job_id


if __name__ == "__main__":
    main()
