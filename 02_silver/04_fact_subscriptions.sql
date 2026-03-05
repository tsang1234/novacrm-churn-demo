-- ============================================================
-- 02_silver / 04_fact_subscriptions.sql
-- Fait abonnements — duration_days et is_current_contract calculés
-- Dépend de : bronze.raw_subscriptions, silver.dim_companies
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.silver.fact_subscriptions
USING DELTA
COMMENT 'Fact subscriptions — duration_days computed, is_current_contract flagged, FKs validated.'
AS
WITH deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY _ingested_at DESC) AS rn
  FROM {catalog}.bronze.raw_subscriptions
  WHERE subscription_id IS NOT NULL AND mrr > 0
),
latest_per_company AS (
  SELECT company_id, MAX(start_date) AS max_start_date
  FROM {catalog}.bronze.raw_subscriptions
  GROUP BY company_id
)
SELECT
  s.subscription_id,
  s.company_id,
  s.plan_type,
  s.billing_cycle,
  s.mrr,
  s.arr,
  CAST(s.start_date AS DATE) AS start_date,
  CAST(s.end_date AS DATE)   AS end_date,
  DATEDIFF(s.end_date, s.start_date) AS duration_days,
  s.status,
  s.contract_version,
  s.auto_renew,
  CASE WHEN s.start_date = lpc.max_start_date THEN TRUE ELSE FALSE END AS is_current_contract
FROM deduped s
INNER JOIN {catalog}.silver.dim_companies dc ON s.company_id = dc.company_id
LEFT  JOIN latest_per_company lpc            ON s.company_id = lpc.company_id
WHERE s.rn = 1;

COMMENT ON TABLE {catalog}.silver.fact_subscriptions IS 'Fait abonnements — historique contrats avec durée et flag contrat courant';
ALTER TABLE {catalog}.silver.fact_subscriptions ALTER COLUMN duration_days       COMMENT 'Durée du contrat en jours (end_date - start_date)';
ALTER TABLE {catalog}.silver.fact_subscriptions ALTER COLUMN is_current_contract COMMENT 'TRUE si ce contrat est le plus récent pour cette entreprise';
ALTER TABLE {catalog}.silver.fact_subscriptions ALTER COLUMN mrr                 COMMENT 'Monthly Recurring Revenue en EUR (entier)';
ALTER TABLE {catalog}.silver.fact_subscriptions ALTER COLUMN arr                 COMMENT 'Annual Recurring Revenue en EUR (mrr * 12)'
