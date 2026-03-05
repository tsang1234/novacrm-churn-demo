-- ============================================================
-- 02_silver / 05_fact_support_tickets.sql
-- Fait tickets support — sentiment catégorisé, is_escalated calculé
-- Dépend de : bronze.raw_support_tickets, silver.dim_companies
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.silver.fact_support_tickets
USING DELTA
COMMENT 'Fact support tickets — sentiment categorized, escalation flagged, FKs validated.'
AS
WITH deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY _ingested_at DESC) AS rn
  FROM {catalog}.bronze.raw_support_tickets
  WHERE ticket_id IS NOT NULL
)
SELECT
  t.ticket_id,
  t.company_id,
  t.contact_id,
  t.category,
  t.priority,
  t.status,
  t.subject,
  t.description,
  CAST(t.sentiment_score AS DOUBLE) AS sentiment_score,
  CASE
    WHEN t.sentiment_score >= 0.2  THEN 'positive'
    WHEN t.sentiment_score <= -0.2 THEN 'negative'
    ELSE 'neutral'
  END AS sentiment_category,
  CAST(t.created_at  AS TIMESTAMP) AS created_at,
  CAST(t.resolved_at AS TIMESTAMP) AS resolved_at,
  CAST(t.ttr_hours   AS INT)       AS ttr_hours,
  t.assigned_agent_id,
  CASE WHEN t.priority IN ('P1','P2') AND t.status IN ('open','in_progress')
       THEN TRUE ELSE FALSE END AS is_escalated
FROM deduped t
INNER JOIN {catalog}.silver.dim_companies dc ON t.company_id = dc.company_id
WHERE t.rn = 1;

COMMENT ON TABLE {catalog}.silver.fact_support_tickets IS 'Fait tickets support — catégorie sentiment calculée, escalade flaggée';
ALTER TABLE {catalog}.silver.fact_support_tickets ALTER COLUMN sentiment_score    COMMENT 'Score de sentiment entre -1 (très négatif) et 1 (très positif)';
ALTER TABLE {catalog}.silver.fact_support_tickets ALTER COLUMN sentiment_category COMMENT 'Catégorie: positive (>=0.2) / neutral / negative (<=-0.2)';
ALTER TABLE {catalog}.silver.fact_support_tickets ALTER COLUMN is_escalated       COMMENT 'TRUE si ticket P1 ou P2 encore ouvert ou en cours de traitement'
