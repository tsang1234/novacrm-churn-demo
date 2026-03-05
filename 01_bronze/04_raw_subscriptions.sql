-- ============================================================
-- 01_bronze / 04_raw_subscriptions.sql
-- ~600 abonnements (1 courant + historique) corrélés au churn_profile
-- Dépend de : 02_raw_companies.sql
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.bronze.raw_subscriptions
USING DELTA
COMMENT 'Raw subscriptions data — NovaCRM billing system export. ~600 rows.'
AS
WITH company_profiles AS (
  SELECT
    id AS company_num,
    CONCAT('CMP-', LPAD(CAST(id AS STRING), 5, '0')) AS company_id,
    CASE WHEN id <= 150 THEN 'slow_decline'
         WHEN id <= 200 THEN 'sudden_stop'
         WHEN id <= 275 THEN 'contract_expiry'
         ELSE 'healthy' END AS churn_profile,
    DATE_ADD(DATE'2020-01-01', pmod(hash(id * 23), 1461)) AS sub_start_date,
    CASE pmod(id, 2) WHEN 0 THEN 'annual' ELSE 'monthly' END AS billing_cycle,
    CASE
      WHEN id <= 150 THEN array('Starter','Business','Business','Enterprise')[pmod(id,4)]
      WHEN id <= 200 THEN array('Starter','Business','Business','Enterprise')[pmod(id,4)]
      WHEN id <= 275 THEN array('Business','Enterprise','Enterprise+')[pmod(id,3)]
      ELSE                array('Business','Enterprise','Enterprise+','Enterprise')[pmod(id,4)]
    END AS plan_type
  FROM (SELECT EXPLODE(SEQUENCE(1, 500)) AS id)
),
-- Contrat courant (1 par entreprise)
current_subs AS (
  SELECT
    company_num, company_id, churn_profile, sub_start_date, billing_cycle, plan_type,
    1 AS contract_version,
    CASE
      WHEN churn_profile = 'slow_decline'    AND pmod(company_num, 5) = 0 THEN 'downgraded'
      WHEN churn_profile = 'sudden_stop'     AND pmod(company_num, 5) <= 1 THEN 'churned'
      WHEN churn_profile = 'contract_expiry' AND pmod(company_num, 3) = 0 THEN 'renewed'
      WHEN churn_profile = 'healthy'         AND pmod(company_num, 10) = 0 THEN 'upgraded'
      WHEN churn_profile = 'healthy'         AND pmod(company_num, 7)  = 0 THEN 'renewed'
      ELSE 'active'
    END AS status,
    ROW_NUMBER() OVER (ORDER BY company_num) AS sub_row
  FROM company_profiles
),
-- Contrats historiques pour les 100 premières entreprises (slow_decline)
hist_subs AS (
  SELECT
    company_num, company_id, churn_profile,
    DATE_ADD(sub_start_date, -365) AS sub_start_date,
    billing_cycle,
    CASE plan_type
      WHEN 'Enterprise+' THEN 'Enterprise'
      WHEN 'Enterprise'  THEN 'Business'
      WHEN 'Business'    THEN 'Starter'
      ELSE 'Starter'
    END AS plan_type,
    2 AS contract_version,
    'churned' AS status,
    500 + ROW_NUMBER() OVER (ORDER BY company_num) AS sub_row
  FROM company_profiles
  WHERE company_num <= 100
),
all_subs AS (
  SELECT * FROM current_subs
  UNION ALL
  SELECT * FROM hist_subs
),
mrr_calc AS (
  SELECT *,
    CASE plan_type
      WHEN 'Starter'    THEN 299  + pmod(hash(company_num * 3), 201)
      WHEN 'Business'   THEN 999  + pmod(hash(company_num * 3), 1501)
      WHEN 'Enterprise' THEN 2999 + pmod(hash(company_num * 3), 5001)
      ELSE                   9999 + pmod(hash(company_num * 3), 20001)
    END AS mrr
  FROM all_subs
)
SELECT
  CONCAT('SUB-', LPAD(CAST(sub_row AS STRING), 6, '0')) AS subscription_id,
  company_id,
  plan_type,
  billing_cycle,
  mrr,
  mrr * 12 AS arr,
  sub_start_date AS start_date,
  CASE billing_cycle
    WHEN 'annual' THEN DATE_ADD(sub_start_date, 365)
    ELSE               DATE_ADD(sub_start_date, 30)
  END AS end_date,
  status,
  contract_version,
  CASE WHEN churn_profile IN ('healthy','contract_expiry') THEN TRUE
       WHEN pmod(company_num, 3) != 0 THEN TRUE
       ELSE FALSE END AS auto_renew,
  CURRENT_TIMESTAMP() AS _ingested_at,
  'billing_system_export' AS _source
FROM mrr_calc
