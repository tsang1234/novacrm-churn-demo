-- ============================================================
-- 02_silver / 01_dim_cs_agents.sql
-- Dimension agents CS — dédupliqué, years_of_service calculé
-- Dépend de : bronze.raw_cs_agents
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.silver.dim_cs_agents
USING DELTA
COMMENT 'Dimension CS agents — cleaned and enriched. years_of_service computed.'
AS
SELECT
  agent_id,
  first_name,
  last_name,
  TRIM(LOWER(email))  AS email,
  region,
  specialization,
  CAST(hire_date AS DATE) AS hire_date,
  ROUND(DATEDIFF(CURRENT_DATE(), CAST(hire_date AS DATE)) / 365.25, 1) AS years_of_service,
  is_active
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY _ingested_at DESC) AS rn
  FROM {catalog}.bronze.raw_cs_agents
  WHERE agent_id IS NOT NULL
)
WHERE rn = 1;

COMMENT ON TABLE {catalog}.silver.dim_cs_agents IS 'Dimension agents Customer Success NovaCRM — ancienneté calculée';
ALTER TABLE {catalog}.silver.dim_cs_agents ALTER COLUMN agent_id COMMENT 'Identifiant agent (format AGT-XXXX)';
ALTER TABLE {catalog}.silver.dim_cs_agents ALTER COLUMN years_of_service COMMENT 'Ancienneté calculée en années (DATEDIFF / 365.25)'
