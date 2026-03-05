-- ============================================================
-- 03_gold / 01_agg_company_health_score.sql
-- Score de santé composite par entreprise
-- usage 30% + support 25% + NPS 20% + paiement 15% + engagement 10%
-- risk_tier : High (<40) / Medium (40-65) / Low (>65)
-- Dépend de : silver.dim_companies, silver.fact_product_events,
--             silver.fact_support_tickets, silver.fact_nps_scores,
--             silver.fact_subscriptions
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.gold.agg_company_health_score
USING DELTA
COMMENT 'Health score composite per company. Weights: usage 30% + support 25% + NPS 20% + payment 15% + engagement 10%.'
AS
WITH usage AS (
  SELECT
    company_id,
    COUNT(CASE WHEN event_date >= DATE_SUB(CURRENT_DATE(), 30) THEN 1 END) AS events_30d,
    COUNT(CASE WHEN event_date >= DATE_SUB(CURRENT_DATE(), 90) THEN 1 END) AS events_90d,
    COUNT(CASE WHEN event_date >= DATE_SUB(CURRENT_DATE(), 60)
                AND event_date <  DATE_SUB(CURRENT_DATE(), 30) THEN 1 END) AS events_30_60d,
    COUNT(DISTINCT CASE WHEN event_date >= DATE_SUB(CURRENT_DATE(), 30) THEN event_type END) AS distinct_features_30d,
    MAX(event_date) AS last_event_date
  FROM {catalog}.silver.fact_product_events
  GROUP BY company_id
),
support AS (
  SELECT
    company_id,
    COUNT(CASE WHEN created_at >= DATE_SUB(CURRENT_DATE(), 30) THEN 1 END) AS tickets_30d,
    COUNT(CASE WHEN created_at >= DATE_SUB(CURRENT_DATE(), 90) THEN 1 END) AS tickets_90d,
    AVG(CASE WHEN created_at >= DATE_SUB(CURRENT_DATE(), 30) THEN sentiment_score END) AS avg_sentiment_30d,
    COUNT(CASE WHEN priority IN ('P1','P2') AND created_at >= DATE_SUB(CURRENT_DATE(), 30) THEN 1 END) AS p1p2_count_30d
  FROM {catalog}.silver.fact_support_tickets
  GROUP BY company_id
),
nps AS (
  SELECT company_id, nps_score AS nps_latest, nps_category
  FROM {catalog}.silver.fact_nps_scores
  QUALIFY ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY survey_date DESC) = 1
),
contracts AS (
  SELECT
    company_id, mrr, status,
    DATEDIFF(end_date, CURRENT_DATE()) AS days_to_renewal,
    CASE WHEN status = 'active' THEN 1.0 WHEN status IN ('downgraded','churned') THEN 0.3 ELSE 0.7 END AS payment_health
  FROM {catalog}.silver.fact_subscriptions
  WHERE is_current_contract = TRUE
),
scored AS (
  SELECT
    c.company_id,
    -- Usage score 0-100
    LEAST(100, GREATEST(0, CAST(
      CASE WHEN COALESCE(u.events_90d,0) = 0 THEN 0
           ELSE COALESCE(u.events_30d,0) * 100.0 / (COALESCE(u.events_90d,1) / 3.0 + 1)
      END AS INT))) AS usage_score,
    -- Support score 0-100
    GREATEST(0, LEAST(100, CAST(100
      - COALESCE(s.tickets_30d,0)  * 5
      - COALESCE(s.p1p2_count_30d,0) * 15
      + COALESCE(s.avg_sentiment_30d,0) * 20 AS INT))) AS support_score,
    -- NPS score normalisé 0-100
    COALESCE(n.nps_latest * 10, 50) AS nps_score_normalized,
    -- Payment score 0-100
    CAST(COALESCE(ct.payment_health, 0.5) * 100 AS INT) AS payment_score,
    -- Engagement trend 0-100
    CASE WHEN COALESCE(u.events_30_60d,0) = 0 THEN 50
         WHEN u.events_30d > u.events_30_60d
         THEN LEAST(100,  50 + CAST((u.events_30d - u.events_30_60d) * 5 AS INT))
         ELSE GREATEST(0, 50 - CAST((u.events_30_60d - u.events_30d) * 5 AS INT))
    END AS engagement_trend,
    COALESCE(u.events_30d, 0)             AS events_30d,
    COALESCE(u.events_90d, 0)             AS events_90d,
    COALESCE(u.distinct_features_30d, 0)  AS distinct_features_30d,
    u.last_event_date,
    COALESCE(s.tickets_30d, 0)            AS tickets_30d,
    COALESCE(s.avg_sentiment_30d, 0.0)    AS avg_sentiment_30d,
    COALESCE(n.nps_latest, 5)             AS nps_latest,
    n.nps_category,
    ct.mrr,
    ct.days_to_renewal,
    ct.status AS contract_status
  FROM {catalog}.silver.dim_companies c
  LEFT JOIN usage     u  ON c.company_id = u.company_id
  LEFT JOIN support   s  ON c.company_id = s.company_id
  LEFT JOIN nps       n  ON c.company_id = n.company_id
  LEFT JOIN contracts ct ON c.company_id = ct.company_id
)
SELECT
  company_id,
  usage_score,
  support_score,
  nps_score_normalized AS nps_score,
  payment_score,
  engagement_trend,
  -- Score composite pondéré
  CAST(usage_score*0.30 + support_score*0.25 + nps_score_normalized*0.20
       + payment_score*0.15 + engagement_trend*0.10 AS INT) AS health_score,
  CASE
    WHEN (usage_score*0.30 + support_score*0.25 + nps_score_normalized*0.20
          + payment_score*0.15 + engagement_trend*0.10) < 40 THEN 'High'
    WHEN (usage_score*0.30 + support_score*0.25 + nps_score_normalized*0.20
          + payment_score*0.15 + engagement_trend*0.10) < 65 THEN 'Medium'
    ELSE 'Low'
  END AS risk_tier,
  events_30d, events_90d, distinct_features_30d, last_event_date,
  tickets_30d, avg_sentiment_30d, nps_latest, nps_category,
  mrr, days_to_renewal, contract_status,
  CURRENT_TIMESTAMP() AS last_computed_at
FROM scored;

COMMENT ON TABLE {catalog}.gold.agg_company_health_score IS 'Score de santé composite par entreprise — usage 30% + support 25% + NPS 20% + paiement 15% + engagement 10%';
ALTER TABLE {catalog}.gold.agg_company_health_score ALTER COLUMN health_score COMMENT 'Score santé composite 0-100';
ALTER TABLE {catalog}.gold.agg_company_health_score ALTER COLUMN risk_tier    COMMENT 'Tier de risque churn: High (<40) / Medium (40-65) / Low (>65)'
