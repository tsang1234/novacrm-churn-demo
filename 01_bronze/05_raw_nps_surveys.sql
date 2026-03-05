-- ============================================================
-- 01_bronze / 05_raw_nps_surveys.sql
-- 2000 enquêtes NPS (4 par entreprise, trimestrielles)
-- Scores corrélés au churn_profile
-- Dépend de : 02_raw_companies.sql, 03_raw_contacts.sql
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.bronze.raw_nps_surveys
USING DELTA
COMMENT 'Raw NPS surveys data — NovaCRM satisfaction platform export. 2000 rows (4 surveys/company).'
AS
WITH company_profiles AS (
  SELECT
    id AS company_num,
    CONCAT('CMP-', LPAD(CAST(id AS STRING), 5, '0')) AS company_id,
    CASE WHEN id <= 150 THEN 'slow_decline'
         WHEN id <= 200 THEN 'sudden_stop'
         WHEN id <= 275 THEN 'contract_expiry'
         ELSE 'healthy' END AS churn_profile,
    DATE_ADD(DATE'2020-01-01', pmod(hash(id * 23), 1461)) AS sub_start_date
  FROM (SELECT EXPLODE(SEQUENCE(1, 500)) AS id)
),
expanded AS (
  SELECT
    cp.company_num,
    cp.company_id,
    cp.churn_profile,
    cp.sub_start_date,
    survey_num,
    -- Toujours le contact décideur principal
    CONCAT('CTT-', LPAD(CAST((cp.company_num - 1) * 4 + 1 AS STRING), 6, '0')) AS contact_id,
    ROW_NUMBER() OVER (ORDER BY cp.company_num, survey_num) AS global_survey_num
  FROM company_profiles cp
  LATERAL VIEW EXPLODE(SEQUENCE(1, 4)) sv AS survey_num
),
nps_scored AS (
  SELECT *,
    -- NPS corrélé au profil de churn (0-10)
    CASE churn_profile
      WHEN 'slow_decline'    THEN GREATEST(0, LEAST(10, CAST(7 - survey_num * 2 + pmod(hash(global_survey_num * 5), 3) AS INT)))
      WHEN 'sudden_stop'     THEN CASE WHEN survey_num >= 4
                                        THEN pmod(hash(global_survey_num * 5), 4)
                                        ELSE 6 + pmod(hash(global_survey_num * 5), 4) END
      WHEN 'contract_expiry' THEN 4 + pmod(hash(global_survey_num * 5), 4)
      ELSE                        7 + pmod(hash(global_survey_num * 5), 4)
    END AS nps_score
  FROM expanded
)
SELECT
  CONCAT('NPS-', LPAD(CAST(global_survey_num AS STRING), 6, '0')) AS survey_id,
  company_id,
  contact_id,
  nps_score,
  CASE WHEN nps_score >= 9 THEN 'promoter'
       WHEN nps_score >= 7 THEN 'passive'
       ELSE 'detractor' END AS nps_category,
  CASE nps_score
    WHEN 10 THEN array('Excellent produit, très satisfait','Outil indispensable, je recommande','Parfait pour notre usage')[pmod(global_survey_num, 3)]
    WHEN 9  THEN array('Très bon outil, quelques améliorations possibles','Satisfait dans l ensemble','Bon rapport qualité-prix')[pmod(global_survey_num, 3)]
    WHEN 8  THEN array('Bien mais quelques bugs','Fonctionnel mais perfectible','Correct, manque certaines fonctions')[pmod(global_survey_num, 3)]
    WHEN 7  THEN array('Moyen, des améliorations nécessaires','Service client lent','Interface vieillissante')[pmod(global_survey_num, 3)]
    WHEN 6  THEN array('Déçu par les performances','Trop de problèmes techniques','Support insuffisant')[pmod(global_survey_num, 3)]
    WHEN 5  THEN array('Envisage des alternatives','Rapport qualité-prix insuffisant','Trop de pannes')[pmod(global_survey_num, 3)]
    WHEN 4  THEN array('Très insatisfait, cherche alternative','Problèmes non résolus depuis des mois','Pas à la hauteur')[pmod(global_survey_num, 3)]
    ELSE         array('Catastrophique, migration en cours','Aucune valeur ajoutée','Service déplorable')[pmod(global_survey_num, 3)]
  END AS comment,
  DATE_ADD(sub_start_date, (survey_num - 1) * 90 + 30) AS survey_date,
  CONCAT('Q', survey_num, '-', YEAR(DATE_ADD(sub_start_date, (survey_num - 1) * 90 + 30))) AS quarter,
  CURRENT_TIMESTAMP() AS _ingested_at,
  'nps_platform_export' AS _source
FROM nps_scored
