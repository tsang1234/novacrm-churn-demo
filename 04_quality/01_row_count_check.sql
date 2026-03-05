-- ============================================================
-- 04_quality / 01_row_count_check.sql
-- Rapport de row counts et vérification data quality
-- Exécuté en dernier — résultat affiché dans run_pipeline.py
-- ============================================================

-- 1. Row counts par couche
SELECT
  layer,
  table_name,
  row_count,
  CASE
    WHEN expected_min > 0 AND row_count < expected_min THEN 'WARN: row count trop bas'
    WHEN row_count = 0 AND expected_min = 0 THEN 'OK (schema vide attendu)'
    ELSE 'OK'
  END AS quality_status
FROM (
  SELECT 'BRONZE' AS layer, 'raw_cs_agents'         AS table_name, COUNT(*) AS row_count, 15     AS expected_min FROM {catalog}.bronze.raw_cs_agents
  UNION ALL SELECT 'BRONZE', 'raw_companies',        COUNT(*), 400   FROM {catalog}.bronze.raw_companies
  UNION ALL SELECT 'BRONZE', 'raw_contacts',         COUNT(*), 1800  FROM {catalog}.bronze.raw_contacts
  UNION ALL SELECT 'BRONZE', 'raw_subscriptions',    COUNT(*), 500   FROM {catalog}.bronze.raw_subscriptions
  UNION ALL SELECT 'BRONZE', 'raw_nps_surveys',      COUNT(*), 1800  FROM {catalog}.bronze.raw_nps_surveys
  UNION ALL SELECT 'BRONZE', 'raw_support_tickets',  COUNT(*), 10000 FROM {catalog}.bronze.raw_support_tickets
  UNION ALL SELECT 'BRONZE', 'raw_product_events',   COUNT(*), 50000 FROM {catalog}.bronze.raw_product_events
  UNION ALL SELECT 'SILVER', 'dim_cs_agents',        COUNT(*), 15    FROM {catalog}.silver.dim_cs_agents
  UNION ALL SELECT 'SILVER', 'dim_companies',        COUNT(*), 400   FROM {catalog}.silver.dim_companies
  UNION ALL SELECT 'SILVER', 'dim_contacts',         COUNT(*), 1800  FROM {catalog}.silver.dim_contacts
  UNION ALL SELECT 'SILVER', 'fact_subscriptions',   COUNT(*), 500   FROM {catalog}.silver.fact_subscriptions
  UNION ALL SELECT 'SILVER', 'fact_support_tickets', COUNT(*), 10000 FROM {catalog}.silver.fact_support_tickets
  UNION ALL SELECT 'SILVER', 'fact_product_events',  COUNT(*), 50000 FROM {catalog}.silver.fact_product_events
  UNION ALL SELECT 'SILVER', 'fact_nps_scores',      COUNT(*), 1800  FROM {catalog}.silver.fact_nps_scores
  UNION ALL SELECT 'GOLD',   'agg_company_health_score', COUNT(*), 400 FROM {catalog}.gold.agg_company_health_score
  UNION ALL SELECT 'GOLD',   'feature_store_churn',      COUNT(*), 400 FROM {catalog}.gold.feature_store_churn
  UNION ALL SELECT 'GOLD',   'agg_churn_predictions',    COUNT(*), 0   FROM {catalog}.gold.agg_churn_predictions
  UNION ALL SELECT 'GOLD',   'agg_retention_actions',    COUNT(*), 0   FROM {catalog}.gold.agg_retention_actions
  UNION ALL SELECT 'GOLD',   'agg_agent_performance',    COUNT(*), 15  FROM {catalog}.gold.agg_agent_performance
)
ORDER BY layer DESC, table_name;

-- 2. Vérification data quality : PKs NOT NULL en Silver
SELECT 'dim_companies PK null'    AS check_name, COUNT(*) AS violations FROM {catalog}.silver.dim_companies    WHERE company_id IS NULL
UNION ALL SELECT 'dim_contacts PK null',         COUNT(*) FROM {catalog}.silver.dim_contacts      WHERE contact_id IS NULL
UNION ALL SELECT 'dim_cs_agents PK null',        COUNT(*) FROM {catalog}.silver.dim_cs_agents     WHERE agent_id   IS NULL
UNION ALL SELECT 'fact_subscriptions PK null',   COUNT(*) FROM {catalog}.silver.fact_subscriptions WHERE subscription_id IS NULL
UNION ALL SELECT 'fact_support_tickets PK null', COUNT(*) FROM {catalog}.silver.fact_support_tickets WHERE ticket_id IS NULL
UNION ALL SELECT 'feature_store is_churned null',COUNT(*) FROM {catalog}.gold.feature_store_churn WHERE is_churned IS NULL;

-- 3. Distribution risk_tier (sanity check du score de santé)
SELECT risk_tier, COUNT(*) AS company_count, ROUND(COUNT(*)*100.0/500,1) AS pct
FROM {catalog}.gold.agg_company_health_score
GROUP BY risk_tier
ORDER BY risk_tier
