-- ============================================================
-- 03_gold / 03_agg_churn_predictions_schema.sql
-- Table vide — recevra les prédictions du modèle ML (MLflow)
-- Alimentée par : 05_ml/03_batch_inference.py
-- ============================================================
CREATE OR REPLACE TABLE {catalog}.gold.agg_churn_predictions
(
  company_id        STRING    COMMENT 'Identifiant entreprise (FK dim_companies)',
  prediction_date   DATE      COMMENT 'Date de la prédiction (batch quotidien)',
  churn_probability DOUBLE    COMMENT 'Probabilité de churn entre 0.0 et 1.0',
  risk_tier         STRING    COMMENT 'Tier de risque: High (>0.7) / Medium (0.4-0.7) / Low (<0.4)',
  top_factors       STRING    COMMENT 'JSON des top SHAP features contribuant à la prédiction',
  model_version     STRING    COMMENT 'Version du modèle MLflow (ex: 3)',
  model_name        STRING    COMMENT 'Nom du modèle dans MLflow Registry (ex: novacrm_churn_model)'
)
USING DELTA
COMMENT 'Prédictions de churn par entreprise — alimentée par le pipeline MLflow (table vide, prête pour le ML)'
