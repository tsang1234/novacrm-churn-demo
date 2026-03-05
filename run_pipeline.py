#!/usr/bin/env python3
"""
NovaCRM Demo Pipeline — Orchestrateur
======================================
Exécute séquentiellement tous les scripts SQL du pipeline Medallion
(Bronze → Silver → Gold) via Databricks SQL Warehouse.

Usage:
    pip install databricks-sdk pyyaml
    python run_pipeline.py
    python run_pipeline.py --catalog my_catalog
    python run_pipeline.py --stage 02_silver          # re-run partiel
    python run_pipeline.py --dry-run                   # affiche les scripts sans les exécuter

Authentification (par ordre de priorité) :
    1. Variables d'environnement : DATABRICKS_HOST + DATABRICKS_TOKEN
    2. Profil CLI dans ~/.databrickscfg (databricks_profile dans config.yml)
    3. Authentification automatique (OAuth, instance profile, etc.)
"""

import argparse
import glob
import os
import sys
import time
import yaml
from pathlib import Path

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
except ImportError:
    print("ERROR: databricks-sdk non installé. Exécutez: pip install databricks-sdk pyyaml")
    sys.exit(1)


# ── Couleurs terminal ──────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


def load_config(config_path: str = "config.yml") -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def build_client(config: dict) -> WorkspaceClient:
    profile = config.get("databricks_profile", "DEFAULT")
    if profile and profile != "DEFAULT":
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()


def find_warehouse(client: WorkspaceClient, warehouse_id: str) -> str:
    if warehouse_id and warehouse_id != "auto":
        return warehouse_id
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("Aucun SQL Warehouse trouvé dans ce workspace.")
    # Préférer un warehouse en cours d'exécution
    running = [w for w in warehouses if str(w.state) in ("RUNNING", "WarehouseState.RUNNING")]
    chosen = running[0] if running else warehouses[0]
    print(f"  Warehouse sélectionné: {CYAN}{chosen.name}{RESET} (id={chosen.id})")
    return chosen.id


def execute_sql(client: WorkspaceClient, warehouse_id: str, sql: str, timeout: int) -> list:
    """Exécute un statement SQL et retourne les résultats."""
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout=f"{timeout}s",
    )
    state = response.status.state
    # Polling si la requête n'est pas terminée immédiatement
    while state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        response = client.statement_execution.get_statement(response.statement_id)
        state = response.status.state

    if state != StatementState.SUCCEEDED:
        error = response.status.error
        raise RuntimeError(f"SQL failed [{state}]: {error.message if error else 'unknown error'}")

    if response.result and response.result.data_array:
        return response.result.data_array
    return []


def run_sql_file(
    client: WorkspaceClient,
    warehouse_id: str,
    sql_file: Path,
    catalog: str,
    timeout: int,
    dry_run: bool,
) -> None:
    """Lit un fichier SQL, substitue les variables et l'exécute."""
    content = sql_file.read_text(encoding="utf-8")
    # Substitution des variables
    content = content.replace("{catalog}", catalog)

    # Découpe en statements (séparés par ;)
    statements = [s.strip() for s in content.split(";") if s.strip() and not s.strip().startswith("--")]

    print(f"  {CYAN}→{RESET} {sql_file.name}  ({len(statements)} statement(s))")

    if dry_run:
        for i, stmt in enumerate(statements, 1):
            print(f"    [{i}] {stmt[:80].replace(chr(10), ' ')}...")
        return

    for stmt in statements:
        execute_sql(client, warehouse_id, stmt, timeout)


def run_stage(
    client: WorkspaceClient,
    warehouse_id: str,
    stage_dir: Path,
    catalog: str,
    timeout: int,
    dry_run: bool,
) -> tuple[int, int]:
    """Exécute tous les fichiers SQL d'un stage. Retourne (ok, errors)."""
    sql_files = sorted(stage_dir.glob("*.sql"))
    if not sql_files:
        return 0, 0

    ok, errors = 0, 0
    for sql_file in sql_files:
        try:
            run_sql_file(client, warehouse_id, sql_file, catalog, timeout, dry_run)
            ok += 1
        except Exception as e:
            print(f"  {RED}✗ ERREUR dans {sql_file.name}:{RESET}")
            print(f"    {e}")
            errors += 1
            raise  # fail-fast

    return ok, errors


def print_row_counts(client: WorkspaceClient, warehouse_id: str, catalog: str, timeout: int) -> None:
    """Affiche le row count de toutes les tables du pipeline."""
    sql = f"""
    SELECT 'BRONZE' AS layer, 'raw_cs_agents'         AS table_name, COUNT(*) AS row_count FROM {catalog}.bronze.raw_cs_agents
    UNION ALL SELECT 'BRONZE', 'raw_companies',        COUNT(*) FROM {catalog}.bronze.raw_companies
    UNION ALL SELECT 'BRONZE', 'raw_contacts',         COUNT(*) FROM {catalog}.bronze.raw_contacts
    UNION ALL SELECT 'BRONZE', 'raw_subscriptions',    COUNT(*) FROM {catalog}.bronze.raw_subscriptions
    UNION ALL SELECT 'BRONZE', 'raw_nps_surveys',      COUNT(*) FROM {catalog}.bronze.raw_nps_surveys
    UNION ALL SELECT 'BRONZE', 'raw_support_tickets',  COUNT(*) FROM {catalog}.bronze.raw_support_tickets
    UNION ALL SELECT 'BRONZE', 'raw_product_events',   COUNT(*) FROM {catalog}.bronze.raw_product_events
    UNION ALL SELECT 'SILVER', 'dim_cs_agents',        COUNT(*) FROM {catalog}.silver.dim_cs_agents
    UNION ALL SELECT 'SILVER', 'dim_companies',        COUNT(*) FROM {catalog}.silver.dim_companies
    UNION ALL SELECT 'SILVER', 'dim_contacts',         COUNT(*) FROM {catalog}.silver.dim_contacts
    UNION ALL SELECT 'SILVER', 'fact_subscriptions',   COUNT(*) FROM {catalog}.silver.fact_subscriptions
    UNION ALL SELECT 'SILVER', 'fact_support_tickets', COUNT(*) FROM {catalog}.silver.fact_support_tickets
    UNION ALL SELECT 'SILVER', 'fact_product_events',  COUNT(*) FROM {catalog}.silver.fact_product_events
    UNION ALL SELECT 'SILVER', 'fact_nps_scores',      COUNT(*) FROM {catalog}.silver.fact_nps_scores
    UNION ALL SELECT 'GOLD',   'agg_company_health_score', COUNT(*) FROM {catalog}.gold.agg_company_health_score
    UNION ALL SELECT 'GOLD',   'feature_store_churn',      COUNT(*) FROM {catalog}.gold.feature_store_churn
    UNION ALL SELECT 'GOLD',   'agg_churn_predictions',    COUNT(*) FROM {catalog}.gold.agg_churn_predictions
    UNION ALL SELECT 'GOLD',   'agg_retention_actions',    COUNT(*) FROM {catalog}.gold.agg_retention_actions
    UNION ALL SELECT 'GOLD',   'agg_agent_performance',    COUNT(*) FROM {catalog}.gold.agg_agent_performance
    ORDER BY layer, table_name
    """
    rows = execute_sql(client, warehouse_id, sql, timeout)
    print(f"\n{BOLD}{'─'*60}{RESET}")
    print(f"{BOLD}  Row counts — {catalog}{RESET}")
    print(f"{BOLD}{'─'*60}{RESET}")
    current_layer = None
    for row in rows:
        layer, table, count = row[0], row[1], int(row[2])
        if layer != current_layer:
            current_layer = layer
            print(f"\n  {BOLD}{layer}{RESET}")
        status = f"{GREEN}✓{RESET}" if count > 0 else f"{YELLOW}○{RESET} (schema vide)"
        print(f"    {status}  {table:<35} {count:>8,} rows")
    print(f"\n{BOLD}{'─'*60}{RESET}")


def main():
    parser = argparse.ArgumentParser(description="NovaCRM Demo Pipeline Orchestrator")
    parser.add_argument("--config",    default="config.yml",  help="Chemin vers config.yml")
    parser.add_argument("--catalog",   default=None,          help="Override du catalog (défaut: config.yml)")
    parser.add_argument("--stage",     default=None,          help="Exécute un seul stage (ex: 02_silver)")
    parser.add_argument("--dry-run",   action="store_true",   help="Affiche les SQL sans les exécuter")
    parser.add_argument("--skip-counts", action="store_true", help="Ne pas afficher les row counts finaux")
    args = parser.parse_args()

    # ── Chargement config ───────────────────────────────────────
    config   = load_config(args.config)
    catalog  = args.catalog or config["catalog"]
    timeout  = config.get("sql_timeout", 600)
    dry_run  = args.dry_run

    print(f"\n{BOLD}{'═'*60}{RESET}")
    print(f"{BOLD}  NovaCRM Demo Pipeline{RESET}")
    print(f"  Catalog : {CYAN}{catalog}{RESET}")
    print(f"  Mode    : {'DRY RUN' if dry_run else 'EXECUTE'}")
    print(f"{BOLD}{'═'*60}{RESET}\n")

    if dry_run:
        client, warehouse_id = None, "dry-run"
    else:
        client       = build_client(config)
        warehouse_id = find_warehouse(client, config.get("warehouse_id", "auto"))

    # ── Résolution des stages à exécuter ───────────────────────
    pipeline_root = Path(__file__).parent
    all_stages    = sorted(config.get("stages", [
        "00_setup", "01_bronze", "02_silver", "03_gold", "04_quality"
    ]))

    if args.stage:
        stages_to_run = [s for s in all_stages if s == args.stage or s.startswith(args.stage)]
        if not stages_to_run:
            print(f"{RED}Stage '{args.stage}' non trouvé.{RESET}")
            sys.exit(1)
    else:
        stages_to_run = all_stages

    # ── Exécution ───────────────────────────────────────────────
    total_ok = 0
    start_time = time.time()

    for stage_name in stages_to_run:
        stage_dir = pipeline_root / stage_name
        if not stage_dir.exists():
            print(f"{YELLOW}  ⚠ Stage {stage_name} introuvable, ignoré.{RESET}")
            continue

        print(f"{BOLD}[{stage_name}]{RESET}")
        try:
            ok, _ = run_stage(client, warehouse_id, stage_dir, catalog, timeout, dry_run)
            total_ok += ok
            print(f"  {GREEN}✓ Stage terminé ({ok} fichier(s)){RESET}\n")
        except Exception:
            elapsed = time.time() - start_time
            print(f"\n{RED}Pipeline interrompu après {elapsed:.1f}s{RESET}")
            sys.exit(1)

    elapsed = time.time() - start_time
    print(f"{GREEN}{BOLD}Pipeline terminé en {elapsed:.1f}s ({total_ok} fichier(s) exécutés){RESET}")

    # ── Row counts finaux ───────────────────────────────────────
    if not dry_run and not args.skip_counts and "04_quality" in stages_to_run:
        try:
            print_row_counts(client, warehouse_id, catalog, timeout)
        except Exception as e:
            print(f"{YELLOW}⚠ Impossible d'afficher les row counts: {e}{RESET}")


if __name__ == "__main__":
    main()
