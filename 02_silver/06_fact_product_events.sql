-- ============================================================
-- 02_silver / 06_fact_product_events.sql
-- Fait événements produit — date et heure extraites, FKs validées
-- Dépend de : bronze.raw_product_events, silver.dim_companies
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.silver.fact_product_events
USING DELTA
COMMENT 'Fact product events — timestamps normalized, event_date and event_hour extracted, FKs validated.'
AS
WITH deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _ingested_at DESC) AS rn
  FROM {catalog}.bronze.raw_product_events
  WHERE event_id IS NOT NULL
)
SELECT
  e.event_id,
  e.company_id,
  e.contact_id,
  e.event_type,
  CAST(e.event_timestamp AS TIMESTAMP) AS event_timestamp,
  DATE(e.event_timestamp)              AS event_date,
  HOUR(e.event_timestamp)              AS event_hour,
  e.session_id,
  e.device_type,
  e.browser
FROM deduped e
INNER JOIN {catalog}.silver.dim_companies dc ON e.company_id = dc.company_id
WHERE e.rn = 1;

COMMENT ON TABLE {catalog}.silver.fact_product_events IS 'Fait événements produit — timestamps UTC normalisés, event_date et event_hour extraits';
ALTER TABLE {catalog}.silver.fact_product_events ALTER COLUMN event_date COMMENT 'Date de l événement (extrait de event_timestamp, utile pour les agrégats journaliers)';
ALTER TABLE {catalog}.silver.fact_product_events ALTER COLUMN event_hour COMMENT 'Heure UTC de l événement (0-23, utile pour l analyse des patterns d usage)'
