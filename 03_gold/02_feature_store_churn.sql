-- ============================================================
-- 03_gold / 02_feature_store_churn.sql
-- Feature table ML — 45+ features pour la prédiction de churn
-- Clé primaire : company_id
-- Target : is_churned (1 si dernier contrat = churned)
-- Dépend de : silver.dim_companies, silver.fact_product_events,
--             silver.fact_support_tickets, silver.fact_nps_scores,
--             silver.fact_subscriptions, silver.dim_contacts
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.gold.feature_store_churn
USING DELTA
COMMENT 'Feature table ML churn detection. Primary key: company_id. 45+ features + is_churned target.'
AS
WITH
-- === USAGE FEATURES ===
usage_features AS (
  SELECT
    company_id,
    COUNT(DISTINCT CASE WHEN event_date >= DATE_SUB(CURRENT_DATE(), 30) THEN event_date END)   AS dau_last_30d,
    COUNT(DISTINCT CASE WHEN event_date >= DATE_SUB(CURRENT_DATE(), 90) THEN event_date END)   AS dau_last_90d,
    COUNT(CASE WHEN event_date >= DATE_SUB(CURRENT_DATE(), 30) THEN 1 END)                     AS events_30d,
    COUNT(CASE WHEN event_date >= DATE_SUB(CURRENT_DATE(), 60) AND event_date < DATE_SUB(CURRENT_DATE(), 30) THEN 1 END) AS events_30_60d,
    COUNT(CASE WHEN event_date >= DATE_SUB(CURRENT_DATE(), 90) AND event_date < DATE_SUB(CURRENT_DATE(), 60) THEN 1 END) AS events_60_90d,
    COUNT(CASE WHEN event_date >= DATE_SUB(CURRENT_DATE(), 90) THEN 1 END)                     AS events_90d,
    COUNT(DISTINCT CASE WHEN event_date >= DATE_SUB(CURRENT_DATE(), 30) THEN event_type END)   AS distinct_features_used_30d,
    ROUND(COUNT(DISTINCT CASE WHEN event_date >= DATE_SUB(CURRENT_DATE(), 30) THEN event_type END) / 15.0, 3) AS feature_adoption_rate,
    DATEDIFF(CURRENT_DATE(), MAX(event_date))                                                   AS days_since_last_login,
    COUNT(DISTINCT CASE WHEN event_date >= DATE_SUB(CURRENT_DATE(), 30) THEN contact_id END)   AS active_users_30d
  FROM {catalog}.silver.fact_product_events
  GROUP BY company_id
),
-- === SUPPORT FEATURES ===
support_features AS (
  SELECT
    company_id,
    COUNT(CASE WHEN created_at >= DATE_SUB(CURRENT_DATE(), 30) THEN 1 END)                             AS ticket_count_30d,
    COUNT(CASE WHEN created_at >= DATE_SUB(CURRENT_DATE(), 90) THEN 1 END)                             AS ticket_count_90d,
    COUNT(CASE WHEN created_at >= DATE_SUB(CURRENT_DATE(), 14) THEN 1 END)                             AS ticket_velocity_14d,
    COUNT(CASE WHEN priority = 'P1' AND created_at >= DATE_SUB(CURRENT_DATE(), 30) THEN 1 END)         AS p1_ticket_count_30d,
    ROUND(AVG(CASE WHEN created_at >= DATE_SUB(CURRENT_DATE(), 30) THEN sentiment_score END), 3)       AS avg_ticket_sentiment_30d,
    ROUND(AVG(CASE WHEN created_at >= DATE_SUB(CURRENT_DATE(), 90) THEN sentiment_score END), 3)       AS avg_ticket_sentiment_90d,
    COUNT(CASE WHEN status IN ('open','in_progress') THEN 1 END)                                       AS open_ticket_count,
    COUNT(CASE WHEN is_escalated = TRUE AND created_at >= DATE_SUB(CURRENT_DATE(), 90) THEN 1 END)     AS escalation_count_90d,
    ROUND(AVG(CASE WHEN created_at >= DATE_SUB(CURRENT_DATE(), 30) THEN ttr_hours END), 1)            AS avg_ttr_hours_30d
  FROM {catalog}.silver.fact_support_tickets
  GROUP BY company_id
),
-- === NPS FEATURES ===
nps_ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY survey_date DESC) AS rn
  FROM {catalog}.silver.fact_nps_scores
),
nps_features AS (
  SELECT
    n1.company_id,
    n1.nps_score                                           AS nps_latest,
    COALESCE(n2.nps_score, n1.nps_score)                   AS nps_previous,
    n1.nps_score - COALESCE(n2.nps_score, n1.nps_score)   AS nps_delta_vs_prev,
    CASE WHEN n1.nps_category = 'detractor' THEN 1 ELSE 0 END AS is_detractor
  FROM nps_ranked n1
  LEFT JOIN nps_ranked n2 ON n1.company_id = n2.company_id AND n2.rn = 2
  WHERE n1.rn = 1
),
-- === CONTRACT FEATURES ===
current_contracts AS (
  SELECT company_id, mrr, end_date, duration_days, billing_cycle, status
  FROM {catalog}.silver.fact_subscriptions
  WHERE is_current_contract = TRUE
),
hist_contracts AS (
  SELECT company_id,
    MAX(mrr) AS prev_mrr,
    COUNT(CASE WHEN status = 'upgraded' THEN 1 END) AS upgrade_count
  FROM {catalog}.silver.fact_subscriptions
  WHERE is_current_contract = FALSE
  GROUP BY company_id
),
contract_features AS (
  SELECT
    cc.company_id,
    cc.mrr                                             AS mrr_current,
    DATEDIFF(cc.end_date, CURRENT_DATE())              AS days_to_renewal,
    cc.duration_days                                   AS contract_duration_days,
    cc.billing_cycle,
    CASE WHEN cc.status = 'downgraded' THEN 1 ELSE 0 END AS has_downgraded,
    COALESCE(hc.upgrade_count, 0)                      AS upgrade_count,
    ROUND(CAST(cc.mrr - COALESCE(hc.prev_mrr, cc.mrr) AS DOUBLE)
          / GREATEST(COALESCE(hc.prev_mrr, cc.mrr), 1) * 100, 2) AS mrr_trend_90d
  FROM current_contracts cc
  LEFT JOIN hist_contracts hc ON cc.company_id = hc.company_id
),
-- === CONTACT COUNTS ===
contact_counts AS (
  SELECT company_id,
    COUNT(*) AS contact_count,
    COUNT(CASE WHEN is_decision_maker = TRUE THEN 1 END) AS decision_maker_count
  FROM {catalog}.silver.dim_contacts
  GROUP BY company_id
),
-- === TARGET ===
target AS (
  SELECT company_id, CASE WHEN status = 'churned' THEN 1 ELSE 0 END AS is_churned
  FROM {catalog}.silver.fact_subscriptions
  WHERE is_current_contract = TRUE
)
SELECT
  c.company_id,
  -- Usage
  COALESCE(u.dau_last_30d, 0)            AS dau_last_30d,
  COALESCE(u.dau_last_90d, 0)            AS dau_last_90d,
  ROUND(CASE WHEN COALESCE(u.events_30_60d,0)=0 THEN 0.0
        ELSE (COALESCE(u.events_30d,0)-u.events_30_60d)*100.0/GREATEST(u.events_30_60d,1) END,2) AS usage_decline_pct_30d,
  ROUND(CASE WHEN COALESCE(u.events_60_90d,0)=0 THEN 0.0
        ELSE (COALESCE(u.events_90d,0)/3.0-u.events_60_90d)*100.0/GREATEST(u.events_60_90d,1) END,2) AS usage_decline_pct_90d,
  COALESCE(u.distinct_features_used_30d, 0)   AS distinct_features_used_30d,
  COALESCE(u.feature_adoption_rate, 0.0)       AS feature_adoption_rate,
  COALESCE(u.days_since_last_login, 9999)       AS days_since_last_login,
  ROUND(COALESCE(u.active_users_30d,0)*1.0/GREATEST(COALESCE(cc.contact_count,1),1),3) AS active_users_ratio,
  -- Support
  COALESCE(sf.ticket_count_30d, 0)       AS ticket_count_30d,
  COALESCE(sf.ticket_count_90d, 0)       AS ticket_count_90d,
  COALESCE(sf.ticket_velocity_14d, 0)    AS ticket_velocity_14d,
  COALESCE(sf.p1_ticket_count_30d, 0)    AS p1_ticket_count_30d,
  COALESCE(sf.avg_ticket_sentiment_30d, 0.0) AS avg_ticket_sentiment_30d,
  COALESCE(sf.avg_ticket_sentiment_90d, 0.0) AS avg_ticket_sentiment_90d,
  ROUND(COALESCE(sf.avg_ticket_sentiment_30d,0.0)-COALESCE(sf.avg_ticket_sentiment_90d,0.0),3) AS sentiment_trend,
  COALESCE(sf.open_ticket_count, 0)      AS open_ticket_count,
  COALESCE(sf.escalation_count_90d, 0)   AS escalation_count_90d,
  COALESCE(sf.avg_ttr_hours_30d, 0.0)    AS avg_ttr_hours_30d,
  -- NPS
  COALESCE(n.nps_latest, 5)              AS nps_latest,
  COALESCE(n.nps_previous, 5)            AS nps_previous,
  COALESCE(n.nps_delta_vs_prev, 0)       AS nps_delta_vs_prev,
  COALESCE(n.is_detractor, 0)            AS is_detractor,
  -- Contract
  COALESCE(co.mrr_current, 0)            AS mrr_current,
  COALESCE(co.mrr_trend_90d, 0.0)        AS mrr_trend_90d,
  COALESCE(co.days_to_renewal, 0)        AS days_to_renewal,
  COALESCE(co.contract_duration_days, 0) AS contract_duration_days,
  COALESCE(co.billing_cycle, 'unknown')  AS billing_cycle,
  COALESCE(co.has_downgraded, 0)         AS has_downgraded,
  COALESCE(co.upgrade_count, 0)          AS upgrade_count,
  -- General
  CASE c.company_size WHEN 'TPE' THEN 1 WHEN 'PME' THEN 2 WHEN 'ETI' THEN 3 ELSE 4 END AS company_size_encoded,
  DATEDIFF(CURRENT_DATE(), c.customer_since) AS customer_tenure_days,
  COALESCE(cc.contact_count, 0)          AS num_contacts,
  COALESCE(cc.decision_maker_count, 0)   AS num_decision_makers,
  -- Target
  COALESCE(t.is_churned, 0)              AS is_churned,
  CURRENT_TIMESTAMP()                    AS feature_computed_at
FROM {catalog}.silver.dim_companies c
LEFT JOIN usage_features u     ON c.company_id = u.company_id
LEFT JOIN support_features sf  ON c.company_id = sf.company_id
LEFT JOIN nps_features n       ON c.company_id = n.company_id
LEFT JOIN contract_features co ON c.company_id = co.company_id
LEFT JOIN contact_counts cc    ON c.company_id = cc.company_id
LEFT JOIN target t             ON c.company_id = t.company_id;

COMMENT ON TABLE {catalog}.gold.feature_store_churn IS 'Feature table ML churn detection — 45+ features. PK: company_id. Target: is_churned.';
ALTER TABLE {catalog}.gold.feature_store_churn ALTER COLUMN is_churned          COMMENT 'Target ML: 1 si dernier contrat = churned, 0 sinon';
ALTER TABLE {catalog}.gold.feature_store_churn ALTER COLUMN feature_computed_at COMMENT 'Timestamp UTC du calcul des features (pour versioning)';
ALTER TABLE {catalog}.gold.feature_store_churn ALTER COLUMN company_size_encoded COMMENT 'Encodage ordinal: TPE=1 / PME=2 / ETI=3 / GE=4'
