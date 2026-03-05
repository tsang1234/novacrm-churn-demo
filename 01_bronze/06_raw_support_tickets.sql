-- ============================================================
-- 01_bronze / 06_raw_support_tickets.sql
-- ~15 000 tickets support — distribution corrélée au churn_profile
-- slow_decline : fréquence croissante, sentiment dégradé
-- sudden_stop  : activité normale puis burst P1/P2 négatifs
-- contract_expiry : tickets modérés, sentiment neutre
-- healthy : tickets rares, sentiment positif
-- Dépend de : 02_raw_companies.sql, 03_raw_contacts.sql
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.bronze.raw_support_tickets
USING DELTA
COMMENT 'Raw support tickets — NovaCRM helpdesk system export. ~15k rows.'
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
    CASE WHEN id <= 150 THEN 45 + pmod(hash(id * 7), 20)   -- 45-65 tickets
         WHEN id <= 200 THEN 35 + pmod(hash(id * 7), 15)   -- 35-50 tickets
         WHEN id <= 275 THEN 22 + pmod(hash(id * 7), 15)   -- 22-37 tickets
         ELSE               12 + pmod(hash(id * 7), 12)    -- 12-24 tickets
    END AS ticket_count
  FROM (SELECT EXPLODE(SEQUENCE(1, 500)) AS id)
),
expanded AS (
  SELECT
    company_num, company_id, churn_profile, sub_start_date, ticket_count,
    ticket_rank,
    pmod(ticket_rank + company_num * 4, 4) + 1 AS contact_rank,
    ROW_NUMBER() OVER (ORDER BY company_num, ticket_rank) AS gtn
  FROM company_profiles
  LATERAL VIEW EXPLODE(SEQUENCE(1, ticket_count)) sv AS ticket_rank
),
tickets AS (
  SELECT *,
    -- Catégorie selon profil et position dans le cycle de vie
    CASE churn_profile
      WHEN 'slow_decline' THEN
        CASE WHEN ticket_rank > ticket_count * 0.6
             THEN array('complaint','billing','bug')[pmod(hash(gtn*5),3)]
             ELSE array('question','bug','feature_request','onboarding')[pmod(hash(gtn*5),4)] END
      WHEN 'sudden_stop' THEN
        CASE WHEN ticket_rank > ticket_count * 0.7
             THEN array('bug','complaint','billing')[pmod(hash(gtn*5),3)]
             ELSE array('question','bug','feature_request','onboarding')[pmod(hash(gtn*5),4)] END
      WHEN 'healthy' THEN array('question','feature_request','onboarding','question','feature_request')[pmod(hash(gtn*5),5)]
      ELSE array('question','bug','feature_request','billing')[pmod(hash(gtn*5),4)]
    END AS category,
    -- Priorité selon profil
    CASE churn_profile
      WHEN 'slow_decline' THEN
        CASE WHEN ticket_rank > ticket_count * 0.6
             THEN array('P1','P2','P2','P3')[pmod(hash(gtn*7),4)]
             ELSE array('P2','P3','P3','P4')[pmod(hash(gtn*7),4)] END
      WHEN 'sudden_stop' THEN
        CASE WHEN ticket_rank > ticket_count * 0.7
             THEN array('P1','P1','P2')[pmod(hash(gtn*7),3)]
             ELSE array('P3','P3','P4','P2')[pmod(hash(gtn*7),4)] END
      WHEN 'healthy' THEN array('P3','P4','P4','P3')[pmod(hash(gtn*7),4)]
      ELSE                array('P2','P3','P3','P4')[pmod(hash(gtn*7),4)]
    END AS priority,
    array('open','resolved','resolved','closed','in_progress','resolved')[pmod(hash(gtn*11),6)] AS status,
    -- Sentiment score entre -1 et 1, corrélé au profil
    CASE churn_profile
      WHEN 'slow_decline' THEN
        ROUND(GREATEST(-1.0, LEAST(1.0,
          CAST(0.3 - 0.8 * CAST(ticket_rank AS DOUBLE)/CAST(ticket_count AS DOUBLE)
            + (pmod(hash(gtn*17),21)-10)/100.0 AS DECIMAL(4,2)))),2)
      WHEN 'sudden_stop' THEN
        CASE WHEN ticket_rank > ticket_count * 0.7
             THEN ROUND(CAST(-0.5 - pmod(hash(gtn*17),4)*0.1 AS DECIMAL(4,2)),2)
             ELSE ROUND(CAST( 0.2 + pmod(hash(gtn*17),3)*0.1 AS DECIMAL(4,2)),2) END
      WHEN 'healthy' THEN ROUND(CAST(0.3 + pmod(hash(gtn*17),4)*0.1 AS DECIMAL(4,2)),2)
      ELSE                ROUND(CAST(-0.1 - pmod(hash(gtn*17),3)*0.1 AS DECIMAL(4,2)),2)
    END AS sentiment_score,
    TIMESTAMP(DATE_ADD(sub_start_date, pmod(hash(gtn*19), 730))) AS created_at,
    pmod(hash(gtn*23), 72) + 1 AS ttr_h
  FROM expanded
)
SELECT
  CONCAT('TKT-', LPAD(CAST(gtn AS STRING), 7, '0')) AS ticket_id,
  company_id,
  CONCAT('CTT-', LPAD(CAST(GREATEST(1, (company_num-1)*4 + contact_rank) AS STRING), 6, '0')) AS contact_id,
  category,
  priority,
  status,
  CONCAT('Ticket #', gtn, ' - ',
    array('Problème performance','Accès impossible','Export échoué','Intégration API','Question facturation','Nouvelle fonctionnalité','Comportement inattendu','Besoin formation')[pmod(hash(gtn*13),8)]
  ) AS subject,
  CONCAT('Le client signale : ',
    array('Temps de réponse lent','Impossible de se connecter','Échec de l export','Timeout API','Facture incorrecte','Demande feature','Bug reproductible','Aide à la prise en main')[pmod(hash(gtn*13),8)]
  ) AS description,
  sentiment_score,
  created_at,
  CASE WHEN status IN ('resolved','closed')
       THEN TIMESTAMP(DATE_ADD(DATE(created_at), CAST(CEIL(ttr_h / 24.0) AS INT) + 1))
       ELSE NULL END AS resolved_at,
  ttr_h AS ttr_hours,
  CONCAT('AGT-', LPAD(CAST(pmod(hash(gtn*29), 15) + 1 AS STRING), 4, '0')) AS assigned_agent_id,
  CURRENT_TIMESTAMP() AS _ingested_at,
  'support_system_api' AS _source
FROM tickets
