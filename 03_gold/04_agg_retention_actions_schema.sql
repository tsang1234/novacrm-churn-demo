-- ============================================================
-- 03_gold / 04_agg_retention_actions_schema.sql
-- Table vide — journal des actions de rétention générées par les agents AI
-- Alimentée par : 06_agents/02_retention_action_agent.py
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.gold.agg_retention_actions
(
  action_id         STRING    COMMENT 'Identifiant unique de l action (UUID)',
  company_id        STRING    COMMENT 'Identifiant entreprise (FK dim_companies)',
  contact_id        STRING    COMMENT 'Contact cible de l action (FK dim_contacts)',
  agent_id          STRING    COMMENT 'Agent CS responsable (FK dim_cs_agents)',
  action_type       STRING    COMMENT 'Type d action: email / call / discount / escalation',
  channel           STRING    COMMENT 'Canal de communication: email / phone / in_app',
  status            STRING    COMMENT 'Statut: pending / approved / sent / completed / rejected',
  generated_content STRING    COMMENT 'Contenu généré par l AI agent (email, offre commerciale, script d appel)',
  created_at        TIMESTAMP COMMENT 'Date de création de l action par l agent AI',
  approved_at       TIMESTAMP COMMENT 'Date de validation par un humain (human-in-the-loop)',
  executed_at       TIMESTAMP COMMENT 'Date d exécution effective de l action',
  outcome           STRING    COMMENT 'Résultat final: saved / churned / pending',
  created_by_agent  STRING    COMMENT 'Nom de l agent AI ayant généré l action (ex: RetentionActionAgent)'
)
USING DELTA
COMMENT 'Journal des actions de rétention générées par les agents AI — vide, prête pour le système multi-agent'
