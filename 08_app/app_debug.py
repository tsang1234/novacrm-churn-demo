"""App de diagnostic pour identifier la cause du 502."""
import os
import sys
import traceback

import streamlit as st

st.set_page_config(page_title="NovaCRM Debug", layout="wide")

st.title("NovaCRM — Diagnostic")
st.success("Streamlit fonctionne !")

# Env vars
st.subheader("1. Variables d'environnement")
wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "NON DEFINI")
host = os.environ.get("DATABRICKS_HOST", "NON DEFINI")
client_id = os.environ.get("DATABRICKS_CLIENT_ID", "NON DEFINI")
st.code(f"DATABRICKS_WAREHOUSE_ID = {wh_id}\nDATABRICKS_HOST = {host}\nDATABRICKS_CLIENT_ID = {client_id[:8]}..." if client_id != "NON DEFINI" else f"DATABRICKS_WAREHOUSE_ID = {wh_id}\nDATABRICKS_HOST = {host}\nDATABRICKS_CLIENT_ID = NON DEFINI")

# SDK Config
st.subheader("2. Databricks SDK Config")
try:
    from databricks.sdk.core import Config
    cfg = Config()
    st.success(f"Config OK — host: {cfg.host}")
except Exception as e:
    st.error(f"Config ERREUR: {e}")
    st.code(traceback.format_exc())
    st.stop()

# SQL Connector import
st.subheader("3. SQL Connector import")
try:
    from databricks import sql
    st.success("databricks-sql-connector importé OK")
except Exception as e:
    st.error(f"Import ERREUR: {e}")
    st.code(traceback.format_exc())
    st.stop()

# Connection test
st.subheader("4. Test connexion SQL Warehouse")
try:
    conn = sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{wh_id}",
        credentials_provider=lambda: cfg.authenticate,
    )
    st.success("Connexion établie !")
except Exception as e:
    st.error(f"Connexion ERREUR: {e}")
    st.code(traceback.format_exc())
    st.stop()

# Query test
st.subheader("5. Test requête SELECT 1")
try:
    cur = conn.cursor()
    cur.execute("SELECT 1 AS test")
    row = cur.fetchone()
    cur.close()
    st.success(f"Query OK ! Résultat: {row}")
except Exception as e:
    st.error(f"Query ERREUR: {e}")
    st.code(traceback.format_exc())
    st.stop()

# Backend import
st.subheader("6. Import backend.py")
try:
    import backend as bk
    st.success("backend.py importé OK")
except Exception as e:
    st.error(f"Import backend ERREUR: {e}")
    st.code(traceback.format_exc())
    st.stop()

# Stats query
st.subheader("7. Test bk.get_stats()")
try:
    stats = bk.get_stats()
    st.success(f"Stats OK: {stats}")
except Exception as e:
    st.error(f"Stats ERREUR: {e}")
    st.code(traceback.format_exc())

# Predictions query
st.subheader("8. Test bk.get_predictions()")
try:
    df = bk.get_predictions()
    st.success(f"Predictions OK: {len(df)} lignes")
except Exception as e:
    st.error(f"Predictions ERREUR: {e}")
    st.code(traceback.format_exc())

# Pending actions
st.subheader("9. Test bk.get_pending_actions()")
try:
    df = bk.get_pending_actions()
    st.success(f"Pending OK: {len(df)} lignes")
except Exception as e:
    st.error(f"Pending ERREUR: {e}")
    st.code(traceback.format_exc())

conn.close()
st.divider()
st.success("Tous les tests sont terminés. Si vous voyez cette ligne, tout fonctionne !")
