-- ============================================================
-- 00_setup / 01_catalog_schemas.sql
-- Crée le catalog Unity Catalog et les 4 schemas Medallion
-- ============================================================
-- Variable : {catalog} → remplacée par run_pipeline.py

CREATE CATALOG IF NOT EXISTS {catalog}
  COMMENT 'NovaCRM Solutions — Demo Databricks Multi-Agent Churn Detection & Resolution';

CREATE SCHEMA IF NOT EXISTS {catalog}.bronze
  COMMENT 'Bronze layer — Raw ingested data from NovaCRM SaaS sources';

CREATE SCHEMA IF NOT EXISTS {catalog}.silver
  COMMENT 'Silver layer — Cleaned, conformed and typed data';

CREATE SCHEMA IF NOT EXISTS {catalog}.gold
  COMMENT 'Gold layer — Business aggregates, feature store and ML-ready tables';

CREATE SCHEMA IF NOT EXISTS {catalog}.ml
  COMMENT 'ML layer — Models, predictions, metrics and monitoring'
