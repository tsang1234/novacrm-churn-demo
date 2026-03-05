-- ============================================================
-- 02_silver / 07_fact_nps_scores.sql
-- Fait scores NPS — catégorie recalculée, FKs validées
-- Dépend de : bronze.raw_nps_surveys, silver.dim_companies
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.silver.fact_nps_scores
USING DELTA
COMMENT 'Fact NPS scores — nps_category recomputed for consistency, FKs validated.'
AS
WITH deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY survey_id ORDER BY _ingested_at DESC) AS rn
  FROM {catalog}.bronze.raw_nps_surveys
  WHERE survey_id IS NOT NULL
    AND nps_score BETWEEN 0 AND 10  -- Data quality check
)
SELECT
  n.survey_id,
  n.company_id,
  n.contact_id,
  CAST(n.nps_score AS INT) AS nps_score,
  -- nps_category recalculée pour garantir la cohérence
  CASE
    WHEN n.nps_score >= 9 THEN 'promoter'
    WHEN n.nps_score >= 7 THEN 'passive'
    ELSE 'detractor'
  END AS nps_category,
  n.comment,
  CAST(n.survey_date AS DATE) AS survey_date,
  n.quarter
FROM deduped n
INNER JOIN {catalog}.silver.dim_companies dc ON n.company_id = dc.company_id
WHERE n.rn = 1;

COMMENT ON TABLE {catalog}.silver.fact_nps_scores IS 'Fait scores NPS — catégorie recalculée (promoter ≥9 / passive ≥7 / detractor <7), FKs validées';
ALTER TABLE {catalog}.silver.fact_nps_scores ALTER COLUMN nps_score    COMMENT 'Score NPS de 0 à 10';
ALTER TABLE {catalog}.silver.fact_nps_scores ALTER COLUMN nps_category COMMENT 'Catégorie recalculée: promoter (9-10) / passive (7-8) / detractor (0-6)';
ALTER TABLE {catalog}.silver.fact_nps_scores ALTER COLUMN quarter      COMMENT 'Trimestre de l enquête (format Q1-2023)'
