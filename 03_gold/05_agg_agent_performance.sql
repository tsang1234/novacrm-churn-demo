-- ============================================================
-- 03_gold / 05_agg_agent_performance.sql
-- KPIs de performance des agents Customer Success
-- Dépend de : silver.dim_cs_agents, silver.dim_companies,
--             silver.fact_subscriptions, gold.agg_company_health_score,
--             silver.fact_support_tickets, silver.fact_nps_scores
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.gold.agg_agent_performance
USING DELTA
COMMENT 'Agent CS performance KPIs — portfolio size, MRR managed, churn rate, save rate, NPS.'
AS
WITH agent_portfolio AS (
  SELECT
    c.assigned_cs_agent_id AS agent_id,
    COUNT(DISTINCT c.company_id)                                                         AS portfolio_size,
    SUM(COALESCE(s.mrr, 0))                                                              AS total_mrr_managed,
    COUNT(DISTINCT CASE WHEN hs.risk_tier = 'High' THEN c.company_id END)               AS high_risk_count
  FROM {catalog}.silver.dim_companies c
  LEFT JOIN {catalog}.silver.fact_subscriptions s          ON c.company_id = s.company_id AND s.is_current_contract = TRUE
  LEFT JOIN {catalog}.gold.agg_company_health_score hs     ON c.company_id = hs.company_id
  GROUP BY c.assigned_cs_agent_id
),
agent_churn AS (
  SELECT
    c.assigned_cs_agent_id AS agent_id,
    ROUND(COUNT(CASE WHEN s.status = 'churned'
                      AND s.start_date >= DATE_SUB(CURRENT_DATE(), 365) THEN 1 END) * 100.0
          / GREATEST(COUNT(DISTINCT c.company_id), 1), 2) AS churn_rate_12m,
    ROUND(COUNT(CASE WHEN s.status IN ('active','renewed','upgraded')
                      AND s.start_date >= DATE_SUB(CURRENT_DATE(), 365) THEN 1 END) * 100.0
          / GREATEST(COUNT(CASE WHEN s.status = 'churned'
                                 AND s.start_date >= DATE_SUB(CURRENT_DATE(), 365) THEN 1 END) + 1, 1), 2) AS save_rate_12m
  FROM {catalog}.silver.dim_companies c
  LEFT JOIN {catalog}.silver.fact_subscriptions s ON c.company_id = s.company_id
  GROUP BY c.assigned_cs_agent_id
),
agent_response AS (
  SELECT assigned_agent_id AS agent_id,
    ROUND(AVG(ttr_hours), 1) AS avg_response_time_hours
  FROM {catalog}.silver.fact_support_tickets
  GROUP BY assigned_agent_id
),
agent_nps AS (
  SELECT c.assigned_cs_agent_id AS agent_id,
    ROUND(AVG(CAST(n.nps_score AS DOUBLE)), 2) AS avg_nps_portfolio
  FROM {catalog}.silver.dim_companies c
  JOIN {catalog}.silver.fact_nps_scores n ON c.company_id = n.company_id
  GROUP BY c.assigned_cs_agent_id
)
SELECT
  a.agent_id,
  COALESCE(ap.portfolio_size, 0)             AS portfolio_size,
  COALESCE(ap.total_mrr_managed, 0)          AS total_mrr_managed,
  COALESCE(ap.high_risk_count, 0)            AS high_risk_count,
  COALESCE(ac.churn_rate_12m, 0.0)           AS churn_rate_12m,
  COALESCE(ac.save_rate_12m, 0.0)            AS save_rate_12m,
  COALESCE(ar.avg_response_time_hours, 0.0)  AS avg_response_time_hours,
  COALESCE(an.avg_nps_portfolio, 5.0)        AS avg_nps_portfolio
FROM {catalog}.silver.dim_cs_agents a
LEFT JOIN agent_portfolio ap ON a.agent_id = ap.agent_id
LEFT JOIN agent_churn     ac ON a.agent_id = ac.agent_id
LEFT JOIN agent_response  ar ON a.agent_id = ar.agent_id
LEFT JOIN agent_nps       an ON a.agent_id = an.agent_id;

COMMENT ON TABLE {catalog}.gold.agg_agent_performance IS 'KPIs performance agents CS — portefeuille MRR, taux churn/save, NPS moyen';
ALTER TABLE {catalog}.gold.agg_agent_performance ALTER COLUMN churn_rate_12m         COMMENT 'Taux de churn 12 mois sur le portefeuille de l agent (%)';
ALTER TABLE {catalog}.gold.agg_agent_performance ALTER COLUMN save_rate_12m          COMMENT 'Taux de rétention des clients à risque sur 12 mois (%)';
ALTER TABLE {catalog}.gold.agg_agent_performance ALTER COLUMN avg_response_time_hours COMMENT 'Temps de résolution moyen des tickets sur 30 jours (heures)'
