-- ============================================================
-- 02_silver / 02_dim_companies.sql
-- Dimension entreprises — churn_profile EXCLU (métadonnée simulation)
-- is_active calculé depuis les contrats courants
-- Dépend de : bronze.raw_companies, bronze.raw_subscriptions
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.silver.dim_companies
USING DELTA
COMMENT 'Dimension companies — cleaned, conformed. churn_profile intentionally excluded (simulation metadata).'
AS
WITH deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY _ingested_at DESC) AS rn
  FROM {catalog}.bronze.raw_companies
  WHERE company_id IS NOT NULL
    AND company_size IN ('TPE','PME','ETI','GE')
    AND base_mrr > 0
),
latest_sub AS (
  SELECT
    company_id,
    CASE WHEN MAX(CASE WHEN status = 'active' THEN 1 ELSE 0 END) = 1 THEN TRUE ELSE FALSE END AS is_active
  FROM {catalog}.bronze.raw_subscriptions
  GROUP BY company_id
)
SELECT
  c.company_id,
  c.company_name,
  c.siren,
  c.sector_naf_code,
  c.sector_label,
  c.company_size,
  c.num_employees,
  c.city,
  c.region,
  c.country,
  c.website,
  CAST(c.subscription_start_date AS DATE) AS customer_since,
  c.plan_type,
  c.base_mrr,
  c.assigned_cs_agent_id,
  COALESCE(ls.is_active, FALSE) AS is_active
  -- NOTE: churn_profile intentionnellement exclu
FROM deduped c
LEFT JOIN latest_sub ls ON c.company_id = ls.company_id
WHERE c.rn = 1;

COMMENT ON TABLE {catalog}.silver.dim_companies IS 'Dimension entreprises clientes NovaCRM — churn_profile exclu (simulation). is_active calculé depuis les contrats.';
ALTER TABLE {catalog}.silver.dim_companies ALTER COLUMN company_id      COMMENT 'Identifiant unique entreprise (format CMP-XXXXX)';
ALTER TABLE {catalog}.silver.dim_companies ALTER COLUMN company_size    COMMENT 'Taille entreprise: TPE / PME / ETI / GE';
ALTER TABLE {catalog}.silver.dim_companies ALTER COLUMN customer_since  COMMENT 'Date début relation client (depuis subscription_start_date bronze)';
ALTER TABLE {catalog}.silver.dim_companies ALTER COLUMN is_active       COMMENT 'TRUE si au moins un contrat actif en cours';
ALTER TABLE {catalog}.silver.dim_companies ALTER COLUMN base_mrr        COMMENT 'MRR de base en EUR (entier, sans centimes)'
