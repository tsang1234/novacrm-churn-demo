-- ============================================================
-- 02_silver / 03_dim_contacts.sql
-- Dimension contacts — emails normalisés, FK company_id validée
-- Dépend de : bronze.raw_contacts, silver.dim_companies
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.silver.dim_contacts
USING DELTA
COMMENT 'Dimension contacts — emails normalized, FK to dim_companies validated.'
AS
SELECT
  c.contact_id,
  c.company_id,
  c.first_name,
  c.last_name,
  TRIM(LOWER(c.email)) AS email,
  c.phone,
  c.role,
  c.is_decision_maker,
  c.is_primary_contact,
  CAST(c.created_at AS TIMESTAMP) AS created_at
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY _ingested_at DESC) AS rn
  FROM {catalog}.bronze.raw_contacts
  WHERE contact_id IS NOT NULL
) c
-- Validation FK dim_companies
INNER JOIN {catalog}.silver.dim_companies dc ON c.company_id = dc.company_id
WHERE c.rn = 1
  AND c.email IS NOT NULL;

COMMENT ON TABLE {catalog}.silver.dim_contacts IS 'Dimension contacts — emails normalisés (lowercase/trim), FKs validées';
ALTER TABLE {catalog}.silver.dim_contacts ALTER COLUMN contact_id        COMMENT 'Identifiant unique contact (format CTT-XXXXXX)';
ALTER TABLE {catalog}.silver.dim_contacts ALTER COLUMN is_decision_maker COMMENT 'TRUE si le contact est un décideur (CEO, CTO, DG, VP...)';
ALTER TABLE {catalog}.silver.dim_contacts ALTER COLUMN is_primary_contact COMMENT 'TRUE si contact principal de l entreprise (1 seul par company)'
