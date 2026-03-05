-- ============================================================
-- 01_bronze / 01_raw_cs_agents.sql
-- 15 agents Customer Success NovaCRM (données RH)
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.bronze.raw_cs_agents
USING DELTA
COMMENT 'Raw CS agents data — NovaCRM internal HR system export'
AS
SELECT
  CONCAT('AGT-', LPAD(CAST(id AS STRING), 4, '0')) AS agent_id,
  first_name,
  last_name,
  LOWER(CONCAT(first_name, '.', last_name, '@novacrm.fr')) AS email,
  region,
  specialization,
  DATE_ADD(DATE'2017-01-01', pmod(hash(id * 31), 2190)) AS hire_date,
  TRUE AS is_active,
  CURRENT_TIMESTAMP() AS _ingested_at,
  'hr_system_export' AS _source
FROM (
  SELECT
    id,
    array('Marie','Jean','Sophie','Pierre','Claire','Thomas','Isabelle','Nicolas','Anne','François','Camille','Laurent','Julie','Sébastien','Nathalie')[id - 1] AS first_name,
    array('Martin','Bernard','Dubois','Leroy','Moreau','Simon','Laurent','Lefebvre','Michel','Garcia','David','Bertrand','Roux','Vincent','Fournier')[id - 1] AS last_name,
    array('Île-de-France','Auvergne-Rhône-Alpes','PACA','Occitanie','Nouvelle-Aquitaine','Grand Est','Hauts-de-France','Bretagne','Normandie','Pays de la Loire','Île-de-France','Auvergne-Rhône-Alpes','Île-de-France','Grand Est','PACA')[id - 1] AS region,
    array('Enterprise','Enterprise','Enterprise','Mid-Market','Mid-Market','Mid-Market','SMB','SMB','SMB','Tech','Tech','Retail','Retail','Enterprise','Mid-Market')[id - 1] AS specialization
  FROM (SELECT EXPLODE(SEQUENCE(1, 15)) AS id)
);

COMMENT ON TABLE {catalog}.bronze.raw_cs_agents IS 'Raw CS agents data — NovaCRM internal HR system export. 15 agents.'
