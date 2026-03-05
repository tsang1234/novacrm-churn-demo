"""
08_app / app.py
================
NovaCRM — Churn Response Dashboard
Application Streamlit Human-in-the-Loop pour la validation des actions de rétention.

Pages :
  1. Dashboard   — KPIs + distribution des risques + activité récente
  2. Review      — Liste des actions en attente, Approuver / Rejeter
  3. Predictions — Vue complète des prédictions ML
"""

import json

import pandas as pd
import streamlit as st

# ── Config page (DOIT être le premier appel Streamlit) ─────────────────────────
st.set_page_config(
    page_title="NovaCRM — Churn Response",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

import backend as bk  # noqa: E402 — après set_page_config

# ── CSS minimal ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: #f0f2f6; border-radius: 8px;
        padding: 16px 20px; text-align: center;
    }
    .metric-card .value { font-size: 2rem; font-weight: 700; color: #1f1f1f; }
    .metric-card .label { font-size: 0.85rem; color: #555; margin-top: 4px; }
    .action-card {
        border: 1px solid #e0e0e0; border-radius: 8px;
        padding: 16px; margin-bottom: 12px;
        background: #fafafa;
    }
    .tag-high    { background:#fee2e2; color:#b91c1c; border-radius:4px; padding:2px 8px; font-size:0.8rem; }
    .tag-email   { background:#dbeafe; color:#1d4ed8; border-radius:4px; padding:2px 8px; font-size:0.8rem; }
    .tag-call    { background:#dcfce7; color:#166534; border-radius:4px; padding:2px 8px; font-size:0.8rem; }
    .tag-discount{ background:#fef9c3; color:#854d0e; border-radius:4px; padding:2px 8px; font-size:0.8rem; }
    .tag-escalation { background:#f3e8ff; color:#6b21a8; border-radius:4px; padding:2px 8px; font-size:0.8rem; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/48/combo-chart.png", width=40)
    st.markdown("## NovaCRM\n**Churn Response**")
    st.divider()
    page = st.radio(
        "Navigation",
        ["🏠 Dashboard", "✅ Review Actions", "📊 Predictions"],
        label_visibility="collapsed",
    )
    st.divider()
    if st.button("🔄 Rafraîchir les données", use_container_width=True):
        st.cache_resource.clear()
        st.rerun()
    st.caption(f"Catalog : `novacrm_demo`")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
if page == "🏠 Dashboard":
    st.title("🏠 Dashboard — Vue d'ensemble")

    with st.spinner("Chargement des indicateurs…"):
        try:
            stats = bk.get_stats()
            risk_df = bk.get_risk_distribution()
            action_df = bk.get_action_distribution()
        except Exception as e:
            st.error(f"Erreur de connexion : {e}")
            st.stop()

    # ── KPIs ──
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("Entreprises analysées", f"{int(stats.get('total_companies', 0) or 0):,}")
    with c2:
        n_high = int(stats.get("high_risk", 0) or 0)
        st.metric("🔴 High Risk", n_high, delta=None)
    with c3:
        n_pending = int(stats.get("pending_actions", 0) or 0)
        st.metric("⏳ Actions en attente", n_pending)
    with c4:
        n_approved = int(stats.get("approved_actions", 0) or 0)
        st.metric("✅ Approuvées", n_approved)
    with c5:
        mrr = stats.get("mrr_at_risk") or 0
        st.metric("💶 MRR à risque", f"{float(mrr):,.0f} €")

    st.divider()

    col_left, col_right = st.columns([1, 1])

    # ── Distribution des risques ──
    with col_left:
        st.subheader("Distribution des risques")
        if not risk_df.empty:
            colors = {"High": "#ef4444", "Medium": "#f97316", "Low": "#22c55e"}
            risk_df["color"] = risk_df["risk_tier"].map(colors).fillna("#94a3b8")

            for _, row in risk_df.iterrows():
                tier = row["risk_tier"]
                n    = int(row["n"])
                prob = float(row["avg_prob"])
                total = risk_df["n"].sum()
                pct = n / total * 100 if total > 0 else 0
                color = colors.get(tier, "#94a3b8")
                st.markdown(
                    f'<div style="display:flex;align-items:center;gap:12px;margin-bottom:8px">'
                    f'<span style="width:80px;font-weight:600;color:{color}">{tier}</span>'
                    f'<div style="flex:1;background:#f0f2f6;border-radius:4px;height:20px">'
                    f'<div style="width:{pct:.0f}%;background:{color};height:20px;border-radius:4px"></div></div>'
                    f'<span style="width:60px;text-align:right">{n} ({pct:.0f}%)</span>'
                    f'<span style="width:80px;text-align:right;color:#888">moy {prob:.1f}%</span>'
                    f'</div>',
                    unsafe_allow_html=True
                )

    # ── Distribution des actions ──
    with col_right:
        st.subheader("Actions par type")
        if not action_df.empty:
            pivot = action_df.pivot_table(
                index="action_type", columns="status", values="n",
                aggfunc="sum", fill_value=0
            ).reset_index()
            st.dataframe(
                pivot,
                use_container_width=True,
                hide_index=True,
            )

    # ── Activité récente ──
    st.divider()
    st.subheader("Activité récente")
    try:
        all_actions = bk.get_all_actions()
        if not all_actions.empty:
            display = all_actions[["company_name", "mrr", "action_type",
                                   "status", "created_at"]].head(10)
            display = display.rename(columns={
                "company_name": "Entreprise",
                "mrr": "MRR (€)",
                "action_type": "Type",
                "status": "Statut",
                "created_at": "Créée le",
            })
            st.dataframe(display, use_container_width=True, hide_index=True)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — REVIEW ACTIONS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "✅ Review Actions":
    st.title("✅ Review Actions — Human-in-the-Loop")

    with st.spinner("Chargement des actions en attente…"):
        try:
            df = bk.get_pending_actions()
        except Exception as e:
            st.error(f"Erreur de connexion : {e}")
            st.stop()

    if df.empty:
        st.success("✨ Aucune action en attente. Tout est traité !")
        st.stop()

    n = len(df)
    mrr_total = pd.to_numeric(df["mrr"], errors="coerce").fillna(0).sum()
    st.markdown(
        f"**{n} action(s) en attente** — MRR cumulé concerné : **{mrr_total:,.0f} €**"
    )

    # Filtres
    col_f1, col_f2, _ = st.columns([1, 1, 2])
    with col_f1:
        filter_type = st.multiselect(
            "Type d'action",
            options=df["action_type"].unique().tolist(),
            default=[],
        )
    with col_f2:
        mrr_max = int(pd.to_numeric(df["mrr"], errors="coerce").fillna(0).max()) + 1
        filter_mrr = st.slider(
            "MRR minimum (€)", 0, mrr_max, 0, step=500
        )

    filtered = df.copy()
    if filter_type:
        filtered = filtered[filtered["action_type"].isin(filter_type)]
    if filter_mrr > 0:
        filtered = filtered[pd.to_numeric(filtered["mrr"], errors="coerce").fillna(0) >= filter_mrr]

    st.markdown(f"*{len(filtered)} action(s) affichée(s)*")
    st.divider()

    # ── Bouton Tout approuver ──
    if st.button(f"✅ Tout approuver ({len(filtered)})", type="primary"):
        with st.spinner("Approbation en cours…"):
            errors = []
            for _, row in filtered.iterrows():
                try:
                    bk.approve_action(row["action_id"])
                except Exception as e:
                    errors.append(f"{row['company_name']}: {e}")
        if errors:
            st.error("\n".join(errors))
        else:
            st.success(f"✅ {len(filtered)} action(s) approuvée(s) !")
            st.rerun()

    st.divider()

    # ── Cartes par action ──
    action_icons = {
        "email": "📧",
        "call": "📞",
        "discount": "🏷️",
        "escalation": "🚨",
    }

    for _, row in filtered.iterrows():
        company   = row.get("company_name", row["company_id"])
        action_id = row["action_id"]
        atype     = row["action_type"]
        mrr       = float(row.get("mrr") or 0)
        churn     = float(row.get("churn_probability") or 0)
        days_r    = row.get("days_to_renewal", "?")
        health    = row.get("health_score", "?")
        content   = row.get("generated_content", "")
        icon      = action_icons.get(atype, "📋")

        # Parse top_factors JSON
        top_factors_raw = row.get("top_factors", "{}")
        try:
            tf = json.loads(top_factors_raw) if isinstance(top_factors_raw, str) else top_factors_raw
            top3 = list(tf.keys())[:3] if tf else []
        except Exception:
            top3 = []

        with st.expander(
            f"{icon} **{company}** — {atype.upper()} | "
            f"MRR {mrr:,.0f}€ | Churn {churn:.1%} | "
            f"Renouvellement J-{days_r}",
            expanded=(mrr > 5000),
        ):
            col_info, col_action = st.columns([2, 1])

            with col_info:
                st.markdown(f"**Entreprise :** {company} ({row.get('company_size','?')} — {row.get('industry','?')})")
                st.markdown(f"**Health score :** {health}/100 | **MRR :** {mrr:,.0f} € | **Renouvellement dans :** {days_r} jours")
                if top3:
                    st.markdown(f"**Top SHAP factors :** {', '.join(top3)}")

                st.markdown("---")
                st.markdown("**Contenu généré :**")
                st.text_area(
                    label="",
                    value=content,
                    height=300,
                    key=f"content_{action_id}",
                    label_visibility="collapsed",
                )

            with col_action:
                st.markdown("**Décision**")
                st.markdown(f"Type : `{atype}`")
                st.markdown(f"Canal : `{row.get('channel','?')}`")
                st.markdown(f"Créé par : `{row.get('created_by_agent','')}`")
                st.markdown("")

                if st.button("✅ Approuver", key=f"approve_{action_id}",
                             type="primary", use_container_width=True):
                    try:
                        bk.approve_action(action_id)
                        st.success("Approuvé !")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

                if st.button("❌ Rejeter", key=f"reject_{action_id}",
                             use_container_width=True):
                    try:
                        bk.reject_action(action_id)
                        st.warning("Rejeté.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — PREDICTIONS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📊 Predictions":
    st.title("📊 Prédictions ML — Vue complète")

    with st.spinner("Chargement des prédictions…"):
        try:
            df = bk.get_predictions()
        except Exception as e:
            st.error(f"Erreur : {e}")
            st.stop()

    if df.empty:
        st.info("Aucune prédiction disponible. Exécutez d'abord le pipeline ML.")
        st.stop()

    # Filtres
    col1, col2, col3 = st.columns(3)
    with col1:
        filter_tier = st.multiselect(
            "Risk Tier",
            options=["High", "Medium", "Low"],
            default=["High"],
        )
    with col2:
        min_prob = st.slider("Churn prob. minimum (%)", 0, 100, 0)
    with col3:
        filter_size = st.multiselect(
            "Taille entreprise",
            options=df["company_size"].dropna().unique().tolist(),
            default=[],
        )

    filtered = df.copy()
    if filter_tier:
        filtered = filtered[filtered["risk_tier"].isin(filter_tier)]
    if min_prob > 0:
        filtered = filtered[pd.to_numeric(filtered["churn_probability"], errors="coerce").fillna(0) * 100 >= min_prob]
    if filter_size:
        filtered = filtered[filtered["company_size"].isin(filter_size)]

    st.markdown(f"**{len(filtered)} entreprise(s)** affichée(s)")

    # Formatage
    display = filtered[[
        "company_name", "industry", "company_size", "mrr",
        "days_to_renewal", "health_score", "churn_probability", "risk_tier",
    ]].copy()
    display["churn_probability"] = (pd.to_numeric(display["churn_probability"], errors="coerce").fillna(0) * 100).round(1)
    display["mrr"] = pd.to_numeric(display["mrr"], errors="coerce").fillna(0).round(0).astype(int)

    display = display.rename(columns={
        "company_name":      "Entreprise",
        "industry":          "Secteur",
        "company_size":      "Taille",
        "mrr":               "MRR (€)",
        "days_to_renewal":   "J. renouvellement",
        "health_score":      "Health",
        "churn_probability": "Churn % ↓",
        "risk_tier":         "Tier",
    })

    # Coloration conditionnelle
    def color_tier(val):
        colors = {"High": "background-color:#fee2e2",
                  "Medium": "background-color:#fef9c3",
                  "Low": "background-color:#dcfce7"}
        return colors.get(val, "")

    styled = display.style.map(color_tier, subset=["Tier"])

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height=600,
    )

    # Export
    csv = display.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Exporter CSV",
        data=csv,
        file_name="novacrm_predictions.csv",
        mime="text/csv",
    )
