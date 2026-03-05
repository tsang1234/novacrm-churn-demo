-- ============================================================
-- 01_bronze / 07_raw_product_events.sql
-- ~87 000 événements produit — usage corrélé au churn_profile
-- slow_decline  : 200 events, concentrés sur première moitié (80/20)
-- sudden_stop   : 140 events, normaux puis quasi-nuls les 2 derniers mois
-- contract_expiry : 130 events, répartis uniformément
-- healthy       : 180 events, répartis uniformément
-- Dépend de : 02_raw_companies.sql, 03_raw_contacts.sql
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.bronze.raw_product_events
USING DELTA
COMMENT 'Raw product usage events — NovaCRM analytics platform export (sampled ~87k rows).'
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
    CASE WHEN id <= 150 THEN 200   -- slow_decline   : déclin progressif
         WHEN id <= 200 THEN 140   -- sudden_stop    : arrêt brutal
         WHEN id <= 275 THEN 130   -- contract_expiry: modéré
         ELSE 180 END AS event_count  -- healthy     : stable
  FROM (SELECT EXPLODE(SEQUENCE(1, 500)) AS id)
),
expanded AS (
  SELECT
    cp.company_num, cp.company_id, cp.churn_profile,
    cp.sub_start_date, cp.event_count,
    ev_rank,
    pmod(ev_rank + cp.company_num * 4, 4) + 1 AS contact_rank,
    ROW_NUMBER() OVER (ORDER BY cp.company_num, ev_rank) AS gen
  FROM company_profiles cp
  LATERAL VIEW EXPLODE(SEQUENCE(1, cp.event_count)) sv AS ev_rank
),
event_dates AS (
  SELECT *,
    -- Distribution temporelle corrélée au profil
    CASE churn_profile
      WHEN 'slow_decline' THEN
        -- 80% des events dans les 12 premiers mois (déclin de l'usage)
        CASE WHEN pmod(hash(gen*3), 10) < 8
             THEN pmod(hash(gen*19), 365)
             ELSE 365 + pmod(hash(gen*19), 365) END
      WHEN 'sudden_stop' THEN
        -- Activité normale 22 mois puis quasi-arrêt les 2 derniers mois
        CASE WHEN pmod(hash(gen*3), 10) < 9
             THEN pmod(hash(gen*19), 660)
             ELSE 660 + pmod(hash(gen*19), 60) END
      ELSE pmod(hash(gen*19), 730)   -- répartition uniforme
    END AS days_offset
  FROM expanded
)
SELECT
  -- UUID synthétique reproductible
  CONCAT(
    LPAD(CAST(pmod(hash(gen),    65536) AS STRING), 8, '0'), '-',
    LPAD(CAST(pmod(hash(gen*2),  65536) AS STRING), 4, '0'), '-',
    LPAD(CAST(pmod(hash(gen*3),  65536) AS STRING), 4, '0'), '-',
    LPAD(CAST(pmod(hash(gen*4),  65536) AS STRING), 4, '0'), '-',
    LPAD(CAST(pmod(hash(gen*5), 999999) AS STRING), 12, '0')
  ) AS event_id,
  company_id,
  CONCAT('CTT-', LPAD(CAST(GREATEST(1, (company_num-1)*4 + contact_rank) AS STRING), 6, '0')) AS contact_id,
  array('dashboard_view','contact_create','deal_create','report_generate','email_send','api_call','search','filter_apply','export_data','import_data','settings_update','user_invite','tag_create','pipeline_view','task_create')[pmod(hash(gen*11),15)] AS event_type,
  TIMESTAMP(DATE_ADD(sub_start_date, days_offset)) AS event_timestamp,
  CONCAT('SES-', LPAD(CAST(pmod(hash(gen*13), 99999) AS STRING), 5, '0')) AS session_id,
  array('desktop','desktop','desktop','mobile','tablet')[pmod(hash(gen*17),5)] AS device_type,
  array('Chrome','Chrome','Chrome','Firefox','Safari','Edge','Chrome')[pmod(hash(gen*19),7)] AS browser,
  CURRENT_TIMESTAMP() AS _ingested_at,
  'analytics_platform_api' AS _source
FROM event_dates
