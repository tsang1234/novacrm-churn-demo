# NovaCRM — Multi-Agent Churn Detection & Resolution

> Demo Databricks de bout en bout : Data Engineering → ML → Agents IA → Human-in-the-Loop
> Entreprise fictive : **NovaCRM Solutions** (CRM SaaS B2B, marché français)

---

## Vue d'ensemble

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DATABRICKS WORKSPACE                                │
│                                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐             │
│  │  BRONZE  │───▶│  SILVER  │───▶│   GOLD   │───▶│    ML    │             │
│  │ 7 tables │    │ 7 tables │    │ 5 tables │    │  schema  │             │
│  └──────────┘    └──────────┘    └────┬─────┘    └────┬─────┘             │
│       │               │               │               │                    │
│       │          SQL Warehouse         │               │                    │
│       │          (Serverless)          ▼               ▼                    │
│       │                        ┌─────────────┐  ┌───────────┐             │
│       │                        │ Health Score │  │  MLflow   │             │
│       │                        │  500 firms   │  │ Registry  │             │
│       │                        └──────┬──────┘  └─────┬─────┘             │
│       │                               │               │                    │
│       │                               ▼               ▼                    │
│       │                        ┌─────────────────────────┐                 │
│       │                        │    AGENT IA (LLM)       │                 │
│       │                        │  Risk Detection Agent   │                 │
│       │                        │  Retention Action Agent  │                 │
│       │                        └────────────┬────────────┘                 │
│       │                                     │                              │
│       │                                     ▼                              │
│       │                        ┌─────────────────────────┐                 │
│       │                        │   DATABRICKS APP        │                 │
│       │                        │   Streamlit HITL        │                 │
│       │                        │   Approve / Reject      │                 │
│       │                        └─────────────────────────┘                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Architecture des données — Medallion

### Flux de données

```
                    run_pipeline.py (orchestrateur)
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
         00_setup      01_bronze      02_silver ───▶ 03_gold ───▶ 04_quality
         (catalog)    (génération)   (nettoyage)   (agrégats)   (validation)
```

### Unity Catalog : `novacrm_demo`

| Schema | Rôle | Tables |
|--------|------|--------|
| `bronze` | Données brutes simulées (Spark SQL pur) | 7 tables |
| `silver` | Nettoyées, typées, conformées | 7 tables |
| `gold` | Agrégats métier, feature store, prédictions | 5 tables |
| `ml` | Modèles MLflow Registry | `novacrm_churn_model` |

### Bronze — Données simulées (112 500+ lignes)

Génération 100% Spark SQL : `EXPLODE(SEQUENCE(...))` + `pmod(hash(...))` pour la variation pseudo-aléatoire.

```
raw_cs_agents ─────────── 15 agents CS (nom, région, spécialisation)
raw_companies ─────────── 500 entreprises (4 profils de churn)
raw_contacts ──────────── 2 000 contacts (4 par entreprise)
raw_subscriptions ─────── 600 contrats (1-2 par entreprise)
raw_nps_surveys ───────── 2 000 NPS (4 par entreprise, biaisé par profil)
raw_support_tickets ───── 16 459 tickets (volume biaisé par profil)
raw_product_events ────── 87 250 événements produit (patterns par profil)
```

**Profils de churn simulés** (colonne `churn_profile` — Bronze uniquement, exclue de Silver) :

| Profil | IDs | Comportement |
|--------|-----|-------------|
| `slow_decline` | 1–150 | Usage qui décline progressivement, NPS bas |
| `sudden_stop` | 151–200 | Usage qui s'effondre brutalement (2 derniers mois) |
| `contract_expiry` | 201–275 | Usage moyen, contrats arrivant à expiration |
| `healthy` | 276–500 | Usage stable, NPS élevé |

### Silver — Dimension & Facts

```
dim_cs_agents ──────── 15   (agent_id, agent_name, region, specialization)
dim_companies ─────── 500   (company_id, sector_label, company_size, base_mrr)
dim_contacts ──────── 2 000 (contact_id, contact_name, role, email)
fact_subscriptions ─── 600  (subscription_id, mrr, status, is_current_contract)
fact_support_tickets ─ 16 459 (ticket_id, priority, sentiment_score)
fact_product_events ── 87 250 (event_id, event_type, event_date)
fact_nps_scores ────── 2 000 (nps_score, nps_category, survey_date)
```

### Gold — Agrégats métier

```
┌──────────────────────────────┐
│ agg_company_health_score     │  500 lignes
│ ─────────────────────────── │
│ Score composite 0-100 :      │
│   usage       30%            │
│   support     25%            │
│   NPS         20%            │
│   paiement    15%            │
│   engagement  10%            │
│                              │
│ risk_tier :                  │
│   High   (<40)               │
│   Medium (40-65)             │
│   Low    (>65)               │
└──────────────────────────────┘

┌──────────────────────────────┐
│ feature_store_churn          │  500 lignes
│ ─────────────────────────── │
│ 33 features ML :             │
│   8 usage, 10 support,       │
│   4 NPS, 6 contract,         │
│   4 general, 1 encoded       │
│ Target : is_churned (0/1)    │
│ Imbalance : 20/500 = 4%     │
└──────────────────────────────┘

┌──────────────────────────────┐
│ agg_churn_predictions        │  500 lignes
│ ─────────────────────────── │
│ churn_probability (0.0-1.0)  │
│ risk_tier (High/Medium/Low)  │
│ top_factors (JSON SHAP)      │
│ model_version, model_name    │
└──────────────────────────────┘

┌──────────────────────────────┐
│ agg_retention_actions        │  20 pending
│ ─────────────────────────── │
│ action_type : email / call / │
│   discount / escalation      │
│ status : pending → approved  │
│   → sent → completed         │
│ generated_content (texte IA) │
│ outcome : saved / churned    │
└──────────────────────────────┘

┌──────────────────────────────┐
│ agg_agent_performance        │  15 lignes
│ ─────────────────────────── │
│ KPIs par agent CS            │
└──────────────────────────────┘
```

### Qualité des données

`04_quality/01_row_count_check.sql` valide 3 axes :
1. **Row counts** : 19 tables vs seuils minimaux attendus
2. **Clés primaires** : NOT NULL sur toutes les PK (Silver)
3. **Distribution risk_tier** : répartition High/Medium/Low cohérente

---

## 2. Machine Learning — LightGBM + MLflow

### Pipeline ML

```
┌────────────────────┐     ┌────────────────────┐     ┌────────────────────┐
│  01_train_model    │────▶│  02_register_model │────▶│  03_batch_infer    │
│                    │     │                    │     │                    │
│  feature_store     │     │  MLflow UC         │     │  Champion model    │
│  → LightGBM       │     │  Registry          │     │  → 500 predictions │
│  → 5-fold CV      │     │  → champion alias  │     │  → SHAP top-5     │
│  → MLflow logging  │     │  → challenger      │     │  → MERGE idempot. │
└────────────────────┘     └────────────────────┘     └────────────────────┘
```

### Modèle : LightGBM

| Paramètre | Valeur |
|-----------|--------|
| Type | `LGBMClassifier` (binary) |
| Features | 33 (8 usage + 10 support + 4 NPS + 6 contract + 4 general + 1 encoded) |
| `n_estimators` | 300 |
| `learning_rate` | 0.03 |
| `num_leaves` | 15 |
| `scale_pos_weight` | ~24 (auto-calculé : n_neg / n_pos) |
| `early_stopping` | 50 rounds |
| Validation | 5-fold StratifiedKFold |
| Seuil de décision | 0.40 (ajusté pour le déséquilibre 4%) |

### MLflow Registry

```
Modèle : novacrm_demo.ml.novacrm_churn_model
│
├── Version N   ← alias "champion" (meilleur cv_roc_auc ≥ 0.60)
├── Version N-1 ← alias "challenger" (rollback possible)
│
│  Tags gouvernance :
│    cv_roc_auc, cv_pr_auc, cv_f1
│    n_samples, n_positives
│    feature_table, promoted_at
│
Expérience : /Shared/novacrm/novacrm_demo_churn_experiment
```

### Batch Inference

```
feature_store_churn (500 entreprises)
        │
        ▼
  Champion Model (MLflow)
        │
        ▼
  Prédictions + SHAP TreeExplainer
        │
        ├─ churn_probability (0.0 - 1.0)
        ├─ risk_tier : Low (<0.40) / Medium (0.40-0.70) / High (>0.70)
        └─ top_factors : JSON des 5 features SHAP les plus influentes
                │
                ▼
  MERGE INTO agg_churn_predictions
  ON (company_id, prediction_date)   ← idempotent
```

**Exemple de `top_factors`** :
```json
{
  "nps_delta_vs_prev": 3.93,
  "mrr_current": -2.20,
  "nps_previous": 1.89,
  "nps_latest": 1.46,
  "days_to_renewal": -0.79
}
```

---

## 3. Agents IA — Système Multi-Agent

### Architecture des agents

```
                    agg_churn_predictions
                    (500 entreprises, risk_tier)
                            │
                            │ filtre High risk
                            ▼
              ┌──────────────────────────┐
              │  Risk Detection Agent    │
              │  (ReAct, max 6 tours)    │
              │                          │
              │  Tools :                 │
              │   • get_company_context  │
              │   • decide_action        │
              │                          │
              │  LLM : Databricks FMAPI  │
              │  (Llama 3.3 70B)         │
              └────────────┬─────────────┘
                           │
                           │ action_type + metadata
                           │ status = 'queued'
                           ▼
              ┌──────────────────────────┐
              │  Retention Action Agent  │
              │  (single-shot)           │
              │                          │
              │  4 system prompts :      │
              │   📧 email               │
              │   📞 call script         │
              │   💰 discount offer      │
              │   🚨 escalation brief    │
              │                          │
              │  LLM : Databricks FMAPI  │
              └────────────┬─────────────┘
                           │
                           │ generated_content (texte)
                           │ status = 'pending'
                           ▼
              ┌──────────────────────────┐
              │  Approval Workflow       │
              │  (Human-in-the-Loop)     │
              │                          │
              │  CLI : 03_approval_wf.py │
              │  App : Streamlit (08_app)│
              │                          │
              │  Approuver / Rejeter     │
              └──────────────────────────┘
```

### Agent 1 : Risk Detection Agent

**Pattern** : ReAct (Reasoning + Acting), boucle max 6 tours
**LLM** : Databricks Foundation Model APIs (`mlflow.deployments`)

| Tool | Rôle |
|------|------|
| `get_company_context` | Récupère contacts décisionnaires, 5 derniers tickets, 2 NPS récents, contrat actuel, agent CS assigné (5 requêtes Silver) |
| `decide_action` | Enregistre la décision structurée : `action_type`, `key_signals` (3), `rationale`, `urgency` |

**Règles de décision** :

| Condition | Action |
|-----------|--------|
| Risque modéré, NPS neutre | `email` |
| Risque élevé, historique tickets | `call` |
| Risque élevé + renouvellement < 60j OU downgrade | `discount` |
| MRR > 5 000 € OU client très stratégique | `escalation` |

### Agent 2 : Retention Action Agent

**Pattern** : Single-shot (1 appel LLM par action)
**Input** : Actions `status='queued'` + contexte complet entreprise (7 requêtes SQL)

| Type | Contenu généré | Contraintes |
|------|---------------|-------------|
| `email` | Email de rétention en français (objet + corps) | Max 280 mots, 1 stat d'usage |
| `call` | Script d'appel (Accroche / Diagnostic / Proposition / Closing / Objections) | Ton naturel, ouverture par remerciement |
| `discount` | Offre commerciale + email + justification interne | Max 20% sans validation manager, engagement 6-12 mois |
| `escalation` | Brief exécutif confidentiel (Résumé / Signaux / Historique / Recommandation) | Max 400 mots, factuel |

### Traçabilité MLflow

Chaque appel agent est tracé via `mlflow.start_span` :
- `company_id`, `churn_probability`, `mrr`
- `action_type`, `urgency`
- Thinking blocks du LLM (si Claude)

---

## 4. Databricks App — Human-in-the-Loop

### Application Streamlit

```
URL : https://novacrm-churn-response-...aws.databricksapps.com

┌─────────────────────────────────────────────────────────────────┐
│  SIDEBAR                │  MAIN CONTENT                        │
│                         │                                       │
│  🎯 NovaCRM             │  ┌─────┬─────┬─────┬─────┬─────┐    │
│  Churn Response         │  │ 500 │ 🔴42│ ⏳20│ ✅ 0│€MRR │    │
│                         │  │firms│high │pend │appr │risk │    │
│  ○ 🏠 Dashboard         │  └─────┴─────┴─────┴─────┴─────┘    │
│  ○ ✅ Review Actions     │                                       │
│  ○ 📊 Predictions       │  ┌───────────────────────────────┐    │
│                         │  │ Distribution des risques      │    │
│  [🔄 Rafraîchir]        │  │ High   ████████░░░ 42 (8%)   │    │
│                         │  │ Medium ██████████░ 192 (38%)  │    │
│  Catalog: novacrm_demo  │  │ Low    ██████████ 266 (53%)   │    │
│                         │  └───────────────────────────────┘    │
└─────────────────────────┴───────────────────────────────────────┘

Page "Review Actions" :
┌─────────────────────────────────────────────────────────────────┐
│  20 action(s) en attente — MRR cumulé : 12 340 €               │
│                                                                  │
│  Filtres : [Type ▼]  [MRR min ───○──── ]                       │
│                                                                  │
│  [✅ Tout approuver (20)]                                       │
│                                                                  │
│  ▸ 🏷️ **Open Solutions** — DISCOUNT | MRR 421€ | Churn 85.2%  │
│  ▸ 📧 **TechCorp** — EMAIL | MRR 1 200€ | Churn 72.1%         │
│  ▸ 📞 **DataVision** — CALL | MRR 3 500€ | Churn 68.9%        │
│  ...                                                             │
│                                                                  │
│  Chaque carte déployée montre :                                 │
│    • Entreprise (taille, secteur)                               │
│    • Health score, MRR, jours avant renouvellement              │
│    • Top SHAP factors                                           │
│    • Contenu IA généré (email/script/offre)                     │
│    • Boutons [✅ Approuver] [❌ Rejeter]                        │
└─────────────────────────────────────────────────────────────────┘
```

### Stack technique

| Composant | Technologie |
|-----------|-------------|
| Framework | Streamlit 1.38 (pré-installé Databricks Apps) |
| Auth | Service Principal (auto-injecté) |
| Data | `databricks-sql-connector` → SQL Warehouse serverless |
| Resource | SQL Warehouse via `valueFrom: sql-warehouse` dans `app.yaml` |
| Compute | 2 vCPU, 6 GB RAM (MEDIUM) |

### Workflow des statuts

```
         Risk Detection          Retention Action         Human Review
              Agent                    Agent               (Streamlit)
               │                        │                      │
               ▼                        ▼                      ▼
            queued ──────────▶ pending ──────────▶ approved ──────▶ completed
                                  │                    │              │
                                  │                    │         ┌────┴────┐
                                  │                    │         ▼         ▼
                                  │                    │       saved    churned
                                  │                    │
                                  └── rejected ◀───────┘
```

---

## 5. Déploiement

### Jobs Databricks

```
run_deploy.py (orchestrateur)
    │
    ├── NovaCRM ML Pipeline (Job ID: 954864606333123)
    │   │
    │   ├── Task 1: train_model      (05_ml/01_train_churn_model.py)
    │   ├── Task 2: register_model   (05_ml/02_register_model.py)     depends_on: 1
    │   └── Task 3: batch_inference  (05_ml/03_batch_inference.py)     depends_on: 2
    │
    └── NovaCRM Agent Workflow (Job ID: 851785850521505)
        │
        ├── Task 1: risk_detection    (06_agents/01_risk_detection_agent.py)
        └── Task 2: retention_action  (06_agents/02_retention_action_agent.py) depends_on: 1
```

Compute : **Serverless** (pas de cluster). Upload sur `/Shared/novacrm/`.

### Databricks App

```bash
# Déploiement
databricks workspace import-dir 08_app /Workspace/Shared/novacrm/08_app
databricks apps deploy novacrm-churn-response \
  --source-code-path /Workspace/Shared/novacrm/08_app

# Logs
databricks apps logs novacrm-churn-response
```

---

## 6. Structure du projet

```
churn/
│
├── config.yml                    ← Configuration globale (catalog, warehouse, LLM endpoint)
├── run_pipeline.py               ← Orchestrateur data pipeline (SQL Warehouse)
├── run_deploy.py                 ← Orchestrateur déploiement jobs Databricks
│
├── 00_setup/
│   └── 01_catalog_schemas.sql    ← CREATE CATALOG + 4 schemas
│
├── 01_bronze/
│   ├── 01_raw_cs_agents.sql      ← 15 agents CS
│   ├── 02_raw_companies.sql      ← 500 entreprises (4 profils churn)
│   ├── 03_raw_contacts.sql       ← 2 000 contacts
│   ├── 04_raw_subscriptions.sql  ← 600 contrats
│   ├── 05_raw_nps_surveys.sql    ← 2 000 NPS
│   ├── 06_raw_support_tickets.sql← 16 459 tickets
│   └── 07_raw_product_events.sql ← 87 250 événements
│
├── 02_silver/
│   ├── 01_dim_cs_agents.sql      ← Dimension agents
│   ├── 02_dim_companies.sql      ← Dimension entreprises (churn_profile exclu)
│   ├── 03_dim_contacts.sql       ← Dimension contacts
│   ├── 04_fact_subscriptions.sql ← Fait contrats
│   ├── 05_fact_support_tickets.sql← Fait tickets support
│   ├── 06_fact_product_events.sql← Fait événements produit
│   └── 07_fact_nps_scores.sql    ← Fait scores NPS
│
├── 03_gold/
│   ├── 01_agg_company_health_score.sql ← Score santé composite
│   ├── 02_feature_store_churn.sql      ← 33 features ML
│   ├── 03_agg_churn_predictions_schema.sql ← Schema prédictions
│   ├── 04_agg_retention_actions_schema.sql ← Schema actions rétention
│   └── 05_agg_agent_performance.sql    ← KPIs par agent
│
├── 04_quality/
│   └── 01_row_count_check.sql    ← Row counts + PK nulls + risk distribution
│
├── 05_ml/
│   ├── 01_train_churn_model.py   ← LightGBM + 5-fold CV + MLflow
│   ├── 02_register_model.py      ← UC Registry, champion/challenger
│   └── 03_batch_inference.py     ← SHAP top-5, MERGE idempotent
│
├── 06_agents/
│   ├── 01_risk_detection_agent.py   ← ReAct agent, 2 tools, 6 tours max
│   ├── 02_retention_action_agent.py ← 4 system prompts, génération contenu
│   └── 03_approval_workflow.py      ← CLI Human-in-the-Loop
│
├── 07_deploy/
│   ├── 02_ml_workflow.py         ← Job Databricks ML (3 tasks)
│   └── 03_agent_workflow.py      ← Job Databricks Agents (2 tasks)
│
└── 08_app/
    ├── app.py                    ← Dashboard Streamlit (3 pages)
    ├── backend.py                ← Data layer (SQL Warehouse)
    ├── app.yaml                  ← Config Databricks Apps
    └── requirements.txt          ← databricks-sql-connector, databricks-sdk
```

---

## 7. Chiffres clés

| Métrique | Valeur |
|----------|--------|
| Entreprises | 500 |
| Churned (target) | 20 (4%) |
| Imbalance ratio | 24:1 |
| Features ML | 33 |
| Seuil de décision | 0.40 |
| Minimum AUC pour promotion | 0.60 |
| Actions générées | 20 pending |
| Types d'action | email, call, discount, escalation |
| LLM | Databricks FMAPI (Llama 3.3 70B) |
| Compute | SQL Warehouse Serverless (tout le pipeline) |
| App | Streamlit sur Databricks Apps (MEDIUM: 2 vCPU, 6 GB) |
