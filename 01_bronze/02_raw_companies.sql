-- ============================================================
-- 01_bronze / 02_raw_companies.sql
-- 500 entreprises clientes NovaCRM avec profils de churn
-- churn_profile : slow_decline 30% | sudden_stop 10% | contract_expiry 15% | healthy 45%
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.bronze.raw_companies
USING DELTA
COMMENT 'Raw companies data — NovaCRM CRM API export. 500 companies.'
AS
WITH base AS (
  SELECT id FROM (SELECT EXPLODE(SEQUENCE(1, 500)) AS id)
),
profiles AS (
  SELECT id,
    CASE WHEN id <= 150 THEN 'slow_decline'
         WHEN id <= 200 THEN 'sudden_stop'
         WHEN id <= 275 THEN 'contract_expiry'
         ELSE 'healthy' END AS churn_profile,
    CASE
      WHEN pmod(hash(id * 17), 100) < 30 THEN 'TPE'
      WHEN pmod(hash(id * 17), 100) < 60 THEN 'PME'
      WHEN pmod(hash(id * 17), 100) < 80 THEN 'ETI'
      ELSE 'GE'
    END AS company_size
  FROM base
),
names AS (
  SELECT id,
    array('Tech','Digital','Smart','Agile','Data','Cloud','Web','Logic','Core','Pro','Fast','Blue','Green','Alpha','Next','Open','Flex','Hyper','Neo','Infra')[pmod(id-1,20)]    AS name_prefix,
    array('Solutions','Systèmes','Corp','Services','Consulting','Group','Partners','Technologies','Software','Dynamics','Networks','Innovations','Analytics','Factory','Labs')[pmod(id-1,15)] AS name_suffix,
    array('62.01Z','62.02A','62.09Z','63.11Z','58.29A','64.19Z','70.22Z','73.11Z','47.91A','46.51Z','72.19Z','85.59B','56.10A','43.21A','33.12Z','78.20Z','82.11Z','86.10Z','41.20A','45.11Z')[pmod(id-1,20)] AS sector_naf_code,
    array('Développement logiciel','Conseil systèmes IT','Activités informatiques','Traitement de données','Édition logiciels','Intermédiation bancaire','Conseil en gestion','Publicité','E-commerce','Commerce gros IT','R&D sciences','Formation pro','Restauration','Installation électrique','Réparation machines','Travail temporaire','Services administratifs','Santé','Construction','Commerce auto')[pmod(id-1,20)] AS sector_label,
    array('Paris','Lyon','Marseille','Toulouse','Bordeaux','Nantes','Strasbourg','Lille','Nice','Rennes','Montpellier','Reims','Saint-Étienne','Toulon','Grenoble','Dijon','Angers','Nîmes','Villeurbanne','Clermont-Ferrand','Le Mans','Aix-en-Provence','Brest','Tours','Limoges','Amiens','Perpignan','Metz','Besançon','Caen')[pmod(id-1,30)] AS city,
    array('Île-de-France','Auvergne-Rhône-Alpes','PACA','Occitanie','Nouvelle-Aquitaine','Pays de la Loire','Grand Est','Hauts-de-France','PACA','Bretagne','Occitanie','Grand Est','Auvergne-Rhône-Alpes','PACA','Auvergne-Rhône-Alpes','Bourgogne-Franche-Comté','Pays de la Loire','Occitanie','Auvergne-Rhône-Alpes','Auvergne-Rhône-Alpes','Pays de la Loire','PACA','Bretagne','Centre-Val de Loire','Nouvelle-Aquitaine','Hauts-de-France','Occitanie','Grand Est','Bourgogne-Franche-Comté','Normandie')[pmod(id-1,30)] AS region
  FROM base
)
SELECT
  CONCAT('CMP-', LPAD(CAST(b.id AS STRING), 5, '0')) AS company_id,
  CONCAT(n.name_prefix, ' ', n.name_suffix,
    CASE WHEN b.id > 300 THEN CONCAT(' ', CAST(pmod(b.id, 99) + 1 AS STRING)) ELSE '' END
  ) AS company_name,
  CONCAT(
    LPAD(CAST(pmod(hash(b.id * 11), 900) + 100 AS STRING), 3, '0'),
    LPAD(CAST(pmod(hash(b.id * 13), 900) + 100 AS STRING), 3, '0'),
    LPAD(CAST(pmod(hash(b.id * 17), 900) + 100 AS STRING), 3, '0')
  ) AS siren,
  n.sector_naf_code,
  n.sector_label,
  p.company_size,
  CASE p.company_size
    WHEN 'TPE' THEN 1  + pmod(hash(b.id * 7), 9)
    WHEN 'PME' THEN 10 + pmod(hash(b.id * 7), 240)
    WHEN 'ETI' THEN 250 + pmod(hash(b.id * 7), 4750)
    ELSE             5000 + pmod(hash(b.id * 7), 45000)
  END AS num_employees,
  n.city,
  n.region,
  'FR' AS country,
  LOWER(CONCAT('www.', n.name_prefix, n.name_suffix, '.fr')) AS website,
  DATE_ADD(DATE'2020-01-01', pmod(hash(b.id * 23), 1461)) AS subscription_start_date,
  CASE p.company_size
    WHEN 'TPE' THEN array('Starter','Business','Business')[pmod(b.id,3)]
    WHEN 'PME' THEN array('Business','Business','Enterprise','Starter')[pmod(b.id,4)]
    WHEN 'ETI' THEN array('Enterprise','Enterprise','Enterprise+','Business')[pmod(b.id,4)]
    ELSE            array('Enterprise+','Enterprise+','Enterprise')[pmod(b.id,3)]
  END AS plan_type,
  CASE p.company_size
    WHEN 'TPE' THEN 299  + pmod(hash(b.id * 5), 201)
    WHEN 'PME' THEN 999  + pmod(hash(b.id * 5), 1501)
    WHEN 'ETI' THEN 2999 + pmod(hash(b.id * 5), 5001)
    ELSE             9999 + pmod(hash(b.id * 5), 20001)
  END AS base_mrr,
  CONCAT('AGT-', LPAD(CAST(pmod(b.id - 1, 15) + 1 AS STRING), 4, '0')) AS assigned_cs_agent_id,
  p.churn_profile,   -- Métadonnée de simulation : sera exclue en Silver
  CURRENT_TIMESTAMP() AS _ingested_at,
  'crm_export_api' AS _source
FROM base b
JOIN profiles p ON b.id = p.id
JOIN names   n ON b.id = n.id
