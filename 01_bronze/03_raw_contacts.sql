-- ============================================================
-- 01_bronze / 03_raw_contacts.sql
-- 2000 contacts (4 par entreprise) — rôles et décideurs réalistes
-- Dépend de : 02_raw_companies.sql
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.bronze.raw_contacts
USING DELTA
COMMENT 'Raw contacts data — NovaCRM CRM API export. ~2000 contacts, 4 per company.'
AS
WITH base AS (
  SELECT
    id,
    FLOOR((id - 1) / 4) + 1   AS company_num,
    pmod(id - 1, 4) + 1        AS contact_rank
  FROM (SELECT EXPLODE(SEQUENCE(1, 2000)) AS id)
)
SELECT
  CONCAT('CTT-', LPAD(CAST(id AS STRING), 6, '0')) AS contact_id,
  CONCAT('CMP-', LPAD(CAST(company_num AS STRING), 5, '0')) AS company_id,
  array('Marie','Jean','Sophie','Pierre','Claire','Thomas','Isabelle','Nicolas','Anne','François','Camille','Laurent','Julie','Sébastien','Nathalie','Aurélie','Maxime','Émilie','Alexandre','Céline')[pmod(id * 7 + contact_rank, 20)] AS first_name,
  array('Martin','Bernard','Dubois','Leroy','Moreau','Simon','Laurent','Lefebvre','Michel','Garcia','David','Bertrand','Roux','Vincent','Fournier','Girard','Bonnet','Dupont','Lambert','Fontaine')[pmod(id * 3 + contact_rank, 20)] AS last_name,
  LOWER(CONCAT(
    array('marie','jean','sophie','pierre','claire','thomas','isabelle','nicolas','anne','francois','camille','laurent','julie','sebastien','nathalie','aurelie','maxime','emilie','alexandre','celine')[pmod(id * 7 + contact_rank, 20)],
    '.',
    array('martin','bernard','dubois','leroy','moreau','simon','laurent','lefebvre','michel','garcia','david','bertrand','roux','vincent','fournier','girard','bonnet','dupont','lambert','fontaine')[pmod(id * 3 + contact_rank, 20)],
    '@',
    array('tech','digital','smart','agile','data','cloud','web','logic','core','pro','fast','blue','green','alpha','next','open','flex','hyper','neo','infra')[pmod(company_num - 1, 20)],
    '.fr'
  )) AS email,
  CONCAT('+33 ', CAST(6 + pmod(id, 4) AS STRING), ' ',
    LPAD(CAST(pmod(hash(id * 13), 100) AS STRING), 2, '0'), ' ',
    LPAD(CAST(pmod(hash(id * 17), 100) AS STRING), 2, '0'), ' ',
    LPAD(CAST(pmod(hash(id * 19), 100) AS STRING), 2, '0'), ' ',
    LPAD(CAST(pmod(hash(id * 23), 100) AS STRING), 2, '0')
  ) AS phone,
  CASE contact_rank
    WHEN 1 THEN array('CEO','CTO','VP Sales','DG')[pmod(id, 4)]
    WHEN 2 THEN array('CTO','CFO','VP Sales','IT Manager')[pmod(id, 4)]
    WHEN 3 THEN array('Project Manager','IT Manager','Operations Manager','Marketing Manager')[pmod(id, 4)]
    ELSE        array('Developer','Analyst','Coordinator','User')[pmod(id, 4)]
  END AS role,
  CASE WHEN contact_rank = 1 THEN TRUE
       WHEN contact_rank = 2 AND pmod(id, 3) = 0 THEN TRUE
       ELSE FALSE END AS is_decision_maker,
  CASE WHEN contact_rank = 1 THEN TRUE ELSE FALSE END AS is_primary_contact,
  CAST(DATE_ADD(DATE'2020-01-01', pmod(hash(id * 29), 1461)) AS TIMESTAMP) AS created_at,
  CURRENT_TIMESTAMP() AS _ingested_at,
  'crm_export_api' AS _source
FROM base
