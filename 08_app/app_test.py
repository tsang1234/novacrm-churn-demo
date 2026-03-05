"""App de test minimal — sans base de données."""
import os
import streamlit as st

st.set_page_config(page_title="Test NovaCRM", layout="wide")

st.title("Test App")
st.write("L'app fonctionne !")
st.write(f"DATABRICKS_WAREHOUSE_ID = `{os.environ.get('DATABRICKS_WAREHOUSE_ID', 'NON DEFINI')}`")
st.write(f"DATABRICKS_HOST = `{os.environ.get('DATABRICKS_HOST', 'NON DEFINI')}`")

# Test import SDK
try:
    from databricks.sdk.core import Config
    cfg = Config()
    st.success(f"SDK Config OK — host: {cfg.host}")
except Exception as e:
    st.error(f"SDK Config ERREUR: {e}")

# Test import SQL connector
try:
    from databricks import sql
    st.success("databricks-sql-connector importé OK")
except Exception as e:
    st.error(f"SQL connector ERREUR: {e}")
