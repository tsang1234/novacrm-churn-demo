# NovaCRM — Multi-Agent Churn Detection & Resolution

Demo complète d'un pipeline data Databricks de bout en bout pour la détection et la résolution du churn client, construite autour d'une entreprise fictive **NovaCRM Solutions** (éditeur CRM SaaS B2B français).

---

## Vue d'ensemble

```
Bronze (raw)  →  Silver (clean)  →  Gold (features/aggregats)
                                         ↓
                               05_ml  : LightGBM + MLflow
                                         ↓
                              06_agents : Claude AI Agents
                                         ↓
                           Databricks App (Human-in-the-loop)
```

Le pipeline couvre l'intégralité de la chaîne de valeur :

| Étape | Contenu |
|-------|---------|
| **Données synthétiques** | 7 tables Bronze générées en Spark SQL pur (87k+ events, 16k+ tickets, 2k contacts) |
| **Architecture Medallion** | Bronze → Silver (nettoyage, conformité, FKs) → Gold (agrégats, feature store) |
| **Feature Store ML** | 45+ features pour un modèle de churn (LightGBM + MLflow) |
| **Agents AI** | 3 agents Claude API (détection, action de rétention, validation humaine) |
| **Portabilité** | 1 fichier `config.yml` pour déployer sur n'importe quel tenant Databricks |

---

## Architecture des données

### Catalog Unity Catalog : `novacrm_demo`

```
novacrm_demo
├── bronze      ← Données brutes telles quelles (_ingested_at, _source)
├── silver      ← Nettoyées, typées, FKs validées, dédupliquées
├── gold        ← Agrégats business, feature store ML, schemas agents
└── ml          ← Réservé aux modèles, expériences MLflow
```

### Tables (19 au total)

#### Bronze — Données synthétiques

| Table | Lignes | Description |
|-------|-------:|-------------|
| `raw_cs_agents` | 15 | Agents Customer Success (15 profils régionaux) |
| `raw_companies` | 500 | Entreprises clientes FR (TPE/PME/ETI/GE) avec `churn_profile` |
| `raw_contacts` | 2 000 | 4 contacts par entreprise, rôles décideurs réalistes |
| `raw_subscriptions` | 600 | Historique contrats (courant + historique) |
| `raw_nps_surveys` | 2 000 | 4 enquêtes NPS par entreprise (trimestrielles) |
| `raw_support_tickets` | ~16 500 | Distribution tickets corrélée au profil de churn |
| `raw_product_events` | ~87 000 | Événements usage produit (15 types d'actions) |

#### Silver — Données nettoyées

| Table | Description |
|-------|-------------|
| `dim_companies` | Entreprises sans `churn_profile` (métadonnée simulation exclue), `is_active` calculé |
| `dim_contacts` | Emails normalisés (lowercase/trim), FK company validée |
| `dim_cs_agents` | `years_of_service` calculé |
| `fact_subscriptions` | `duration_days` + `is_current_contract` calculés |
| `fact_support_tickets` | `sentiment_category` (positive/neutral/negative) + `is_escalated` |
| `fact_product_events` | `event_date` et `event_hour` extraits |
| `fact_nps_scores` | `nps_category` recalculée pour cohérence |

#### Gold — Agrégats & ML

| Table | Lignes | Description |
|-------|-------:|-------------|
| `agg_company_health_score` | 500 | Score santé 0-100, `risk_tier` High/Medium/Low |
| `feature_store_churn` | 500 | **45+ features ML** + target `is_churned` |
| `agg_churn_predictions` | 0 | Schema prêt pour les prédictions ML |
| `agg_retention_actions` | 0 | Schema prêt pour les agents AI |
| `agg_agent_performance` | 15 | KPIs agents CS (portfolio, churn rate, NPS) |

### Profils de churn simulés

Les données synthétiques reproduisent 4 comportements réalistes :

| Profil | % du parc | Comportement usage | Comportement tickets |
|--------|----------:|-------------------|---------------------|
| `slow_decline` | 30% | Activité qui diminue progressivement (100% → 20%) | Fréquence croissante, sentiment dégradé mois après mois |
| `sudden_stop` | 10% | Activité normale puis quasi-nulle les 2 derniers mois | Burst de tickets P1/P2 très négatifs en fin de vie |
| `contract_expiry` | 15% | Activité modérée, stable | Tickets modérés, sentiment neutre à légèrement négatif |
| `healthy` | 45% | Activité stable ou légèrement croissante | Tickets rares, sentiment positif |

> **Note** : `churn_profile` n'existe que dans `bronze.raw_companies`. Il est intentionnellement exclu de Silver et Gold — c'est une métadonnée de simulation, pas une feature ML.

---

## Structure du projet

```
novacrm_demo_pipeline/
├── config.yml                              ← Configuration tenant (à modifier)
├── run_pipeline.py                         ← Orchestrateur Python SDK
│
├── 00_setup/
│   └── 01_catalog_schemas.sql             ← CREATE CATALOG + 4 schemas
│
├── 01_bronze/                             ← Génération des données synthétiques
│   ├── 01_raw_cs_agents.sql
│   ├── 02_raw_companies.sql
│   ├── 03_raw_contacts.sql
│   ├── 04_raw_subscriptions.sql
│   ├── 05_raw_nps_surveys.sql
│   ├── 06_raw_support_tickets.sql
│   └── 07_raw_product_events.sql
│
├── 02_silver/                             ← Nettoyage et conformité
│   ├── 01_dim_cs_agents.sql
│   ├── 02_dim_companies.sql
│   ├── 03_dim_contacts.sql
│   ├── 04_fact_subscriptions.sql
│   ├── 05_fact_support_tickets.sql
│   ├── 06_fact_product_events.sql
│   └── 07_fact_nps_scores.sql
│
├── 03_gold/                               ← Agrégats business et feature store
│   ├── 01_agg_company_health_score.sql
│   ├── 02_feature_store_churn.sql
│   ├── 03_agg_churn_predictions_schema.sql
│   ├── 04_agg_retention_actions_schema.sql
│   └── 05_agg_agent_performance.sql
│
├── 04_quality/
│   └── 01_row_count_check.sql             ← Row counts + data quality checks
│
├── 05_ml/                                 ← Pipeline ML (stubs à compléter)
│   ├── 01_train_churn_model.py            ← Entraînement LightGBM + MLflow
│   ├── 02_register_model.py               ← Promotion Model Registry
│   └── 03_batch_inference.py             ← Scoring batch + SHAP
│
└── 06_agents/                             ← Agents AI (stubs à compléter)
    ├── 01_risk_detection_agent.py         ← Détection et triage des High Risk
    ├── 02_retention_action_agent.py       ← Génération email/call/discount
    └── 03_approval_workflow.py            ← Human-in-the-loop validation
```

> L'ordre d'exécution est garanti par la numérotation des dossiers (`00` → `06`) et le tri alphabétique des fichiers à l'intérieur de chaque dossier.

---

## Prérequis

### Databricks
- Workspace Databricks avec **Unity Catalog activé**
- Au moins un **SQL Warehouse** (Serverless ou Classic)
- Droits `CREATE CATALOG` ou un catalog existant avec droits `CREATE SCHEMA`

### Local
- Python 3.9+
- Databricks CLI configuré (`~/.databrickscfg`) **ou** variables d'environnement

```bash
# Option A — Databricks CLI
databricks configure

# Option B — Variables d'environnement
export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=dapi...
```

---

## Installation

```bash
# 1. Cloner / copier le projet
git clone <repo> novacrm_demo_pipeline
cd novacrm_demo_pipeline

# 2. Installer les dépendances Python
pip install databricks-sdk pyyaml

# 3. (Optionnel) Pour les étapes ML
pip install lightgbm scikit-learn shap mlflow

# 4. (Optionnel) Pour les agents AI
pip install anthropic mlflow

# 5. Configurer la clé API Anthropic
export ANTHROPIC_API_KEY=sk-ant-...
```

---

## Configuration

Modifier `config.yml` selon votre tenant :

```yaml
# Nom du catalog Unity Catalog cible
catalog: novacrm_demo

# ID du SQL Warehouse ('auto' pour sélection automatique)
# Databricks UI > SQL Warehouses > Connection details > HTTP path
warehouse_id: auto

# Profil Databricks CLI (défaut si non configuré)
databricks_profile: DEFAULT

# Timeout par requête SQL en secondes
sql_timeout: 600

# Stages à exécuter (retirer un stage pour un re-run partiel)
stages:
  - 00_setup
  - 01_bronze
  - 02_silver
  - 03_gold
  - 04_quality
```

---

## Utilisation

### Pipeline complet

```bash
python run_pipeline.py
```

### Options disponibles

```bash
# Override du catalog (sans modifier config.yml)
python run_pipeline.py --catalog mon_autre_catalog

# Re-run d'un seul stage
python run_pipeline.py --stage 02_silver
python run_pipeline.py --stage 03_gold

# Afficher les SQL sans les exécuter
python run_pipeline.py --dry-run

# Désactiver l'affichage des row counts finaux
python run_pipeline.py --skip-counts
```

### Pipeline ML (étape 5)

```bash
# Entraîner le modèle et logguer dans MLflow
python 05_ml/01_train_churn_model.py

# Promouvoir le meilleur modèle en champion
python 05_ml/02_register_model.py
# ou avec un run précis :
python 05_ml/02_register_model.py --run-id abc123def456

# Scoring batch des 500 entreprises → gold.agg_churn_predictions
python 05_ml/03_batch_inference.py
```

### Agents AI (étape 6)

```bash
# 1. Analyse et triage des entreprises High Risk par Claude (ReAct + tool_use)
export ANTHROPIC_API_KEY=sk-ant-...
python 06_agents/01_risk_detection_agent.py
python 06_agents/01_risk_detection_agent.py --tier High --mrr-min 2000
python 06_agents/01_risk_detection_agent.py --dry-run   # analyse sans générer

# 2. Générer une action manuellement pour une entreprise
python 06_agents/02_retention_action_agent.py --company-id C001 --action-type email
python 06_agents/02_retention_action_agent.py --company-id C042 --action-type discount

# 3. Validation humaine des actions (interface CLI interactive)
python 06_agents/03_approval_workflow.py
python 06_agents/03_approval_workflow.py --tier High --limit 5
python 06_agents/03_approval_workflow.py --agent-id AG001   # portefeuille d'un agent CS
python 06_agents/03_approval_workflow.py --non-interactive  # approuve tout (tests)

# Mettre à jour l'outcome après exécution
python 06_agents/03_approval_workflow.py \
    --update-outcome <action_id> --outcome saved
```

### Exemple de sortie

```
════════════════════════════════════════════════════════════
  NovaCRM Demo Pipeline
  Catalog : novacrm_demo
  Mode    : EXECUTE
════════════════════════════════════════════════════════════

  Warehouse sélectionné: Shared Serverless (id=abc123)

[00_setup]
  → 01_catalog_schemas.sql  (5 statements)
  ✓ Stage terminé (1 fichier(s))

[01_bronze]
  → 01_raw_cs_agents.sql  (1 statement)
  → 02_raw_companies.sql  (1 statement)
  ...
  ✓ Stage terminé (7 fichier(s))

...

Pipeline terminé en 142.3s (20 fichier(s) exécutés)

────────────────────────────────────────────────────────────
  Row counts — novacrm_demo
────────────────────────────────────────────────────────────

  BRONZE
    ✓  raw_cs_agents                             15 rows
    ✓  raw_companies                            500 rows
    ✓  raw_contacts                           2,000 rows
    ✓  raw_subscriptions                        600 rows
    ✓  raw_nps_surveys                        2,000 rows
    ✓  raw_support_tickets                   16,459 rows
    ✓  raw_product_events                    87,250 rows

  SILVER
    ✓  dim_companies                            500 rows
    ...

  GOLD
    ✓  feature_store_churn                      500 rows
    ○  agg_churn_predictions    (schema vide - ready for ML/agents)
    ○  agg_retention_actions    (schema vide - ready for ML/agents)
```

---

## Déployer sur un autre tenant

Le pipeline est **100% paramétrable** via `config.yml`. Pour migrer :

1. Modifier `catalog` dans `config.yml`
2. S'assurer que le profil CLI pointe vers le bon workspace
3. Lancer `python run_pipeline.py`

Toutes les références au catalog dans les SQL sont des placeholders `{catalog}` substitués dynamiquement par l'orchestrateur — aucun fichier SQL à modifier.

---

## Choix techniques

### Génération de données en Spark SQL pur

Les données synthétiques sont générées entièrement en SQL (sans Python ni Faker), ce qui permet d'exécuter le pipeline via un **SQL Warehouse uniquement**, sans cluster Databricks.

- `EXPLODE(SEQUENCE(1, N))` pour générer N lignes
- `pmod(hash(id * seed), N)` pour des valeurs pseudo-aléatoires déterministes
- `LATERAL VIEW EXPLODE(SEQUENCE(1, count))` pour les tables à cardinalité variable (tickets, events)

Les données sont **reproductibles** : les mêmes seeds produisent toujours les mêmes données.

### Health Score composite

Le score de santé (`agg_company_health_score`) est une moyenne pondérée de 5 dimensions :

```
health_score = usage    × 30%
             + support  × 25%
             + NPS      × 20%
             + payment  × 15%
             + engagement × 10%
```

| Score | Risk Tier |
|-------|-----------|
| < 40  | 🔴 High   |
| 40-65 | 🟡 Medium |
| > 65  | 🟢 Low    |

### Feature Store (45+ features)

La table `gold.feature_store_churn` est la source unique pour le modèle ML :

| Groupe | Features clés |
|--------|---------------|
| Usage (8) | `dau_last_30d`, `usage_decline_pct_30d`, `feature_adoption_rate`, `days_since_last_login`, `active_users_ratio` |
| Support (10) | `ticket_count_30d`, `p1_ticket_count_30d`, `avg_ticket_sentiment_30d`, `sentiment_trend`, `escalation_count_90d` |
| NPS (4) | `nps_latest`, `nps_delta_vs_prev`, `is_detractor` |
| Contrat (7) | `mrr_current`, `mrr_trend_90d`, `days_to_renewal`, `has_downgraded`, `upgrade_count` |
| Général (4) | `company_size_encoded`, `customer_tenure_days`, `num_contacts`, `num_decision_makers` |
| **Target** | `is_churned` (1 si dernier contrat = `churned`) |

---

## Roadmap

### Étape 5 — Pipeline ML (`05_ml/`) ✅

- [x] `01_train_churn_model.py` — LightGBM + 5-fold CV + MLflow (scale_pos_weight=24 pour 4% churn)
- [x] `02_register_model.py` — Promotion UC Registry avec alias champion/challenger + tags gouvernance
- [x] `03_batch_inference.py` — Scoring batch 500 entreprises + SHAP top-5 → MERGE dans `gold.agg_churn_predictions`

### Étape 6 — Agents AI (`06_agents/`) ✅

- [x] `01_risk_detection_agent.py` — Agent Claude (ReAct + tool_use) : triage High Risk, décision email/call/discount/escalation, tracing MLflow
- [x] `02_retention_action_agent.py` — Agent Claude : génère contenu personnalisé (email, script appel, offre, brief escalade) → MERGE dans `gold.agg_retention_actions`
- [x] `03_approval_workflow.py` — CLI Human-in-the-loop : Approuver / Rejeter / Ignorer, dashboard stats, update outcomes

### Étape 7 — Databricks App

- [x] Tableau de bord Streamlit pour les agents CS
- [x] Vue portefeuille avec filtres par risk_tier
- [x] Interface de validation des actions (approve / modify / reject)
- [x] Métriques de performance des agents AI (taux de sauvegarde, ROI)

---

## Data Quality

Le script `04_quality/01_row_count_check.sql` vérifie :

1. **Row counts** par table avec seuils minimaux attendus
2. **PKs NOT NULL** sur toutes les tables Silver
3. **Distribution risk_tier** pour valider la cohérence du health score

Pour re-exécuter uniquement les checks :

```bash
python run_pipeline.py --stage 04_quality
```

---

## Contraintes et conventions

| Convention | Détail |
|-----------|--------|
| Format storage | Delta partout |
| Timestamps | UTC |
| Montants | EUR, entiers (pas de centimes) |
| Colonnes `_prefixées` | Bronze uniquement (`_ingested_at`, `_source`) |
| `churn_profile` | Bronze uniquement — exclu Silver/Gold |
| Seed reproductibilité | `pmod(hash(id * seed), N)` — déterministe |
| Commentaires UC | Tables et colonnes clés dans Unity Catalog |

---

## Contribuer / Étendre

### Ajouter une nouvelle table Bronze

1. Créer `01_bronze/08_ma_nouvelle_table.sql` avec `{catalog}` comme placeholder
2. Relancer : `python run_pipeline.py --stage 01_bronze`

### Modifier le catalog cible

```bash
python run_pipeline.py --catalog novacrm_prod
```

### Tester sans exécuter

```bash
python run_pipeline.py --dry-run
```

---

## Licence

Projet de démonstration interne — données entièrement fictives.
NovaCRM Solutions n'existe pas. Toute ressemblance avec une entreprise réelle serait fortuite.
