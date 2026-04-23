"""
Dashboard de leilões da Caixa Econômica Federal.
Versão sem mapa interativo (substituído por agregação por cidade).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

DATA_FILE = Path(__file__).parent / "data" / "imoveis.parquet"

st.set_page_config(
    page_title="Leilões Caixa",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    if not DATA_FILE.exists():
        return pd.DataFrame()
    return pd.read_parquet(DATA_FILE)


df = load_data()

if df.empty:
    st.error(
        "Não há dados ainda. Coloca CSVs na pasta `uploads/` e corre:\n\n"
        "```\npython process_local.py\n```"
    )
    st.stop()


# ---------- Sidebar ----------------------------------------------------------

st.sidebar.title("🔍 Filtros")

ufs_disponiveis = sorted(df["uf"].dropna().unique().tolist())
ufs = st.sidebar.multiselect("Estado (UF)", ufs_disponiveis, default=ufs_disponiveis)

if ufs:
    cidades_disponiveis = sorted(df[df["uf"].isin(ufs)]["cidade"].dropna().unique().tolist())
else:
    cidades_disponiveis = sorted(df["cidade"].dropna().unique().tolist())

cidades = st.sidebar.multiselect("Cidade", cidades_disponiveis, default=[])

tipos_disponiveis = sorted(df["tipo_imovel"].dropna().unique().tolist())
tipos = st.sidebar.multiselect("Tipo de imóvel", tipos_disponiveis, default=tipos_disponiveis)

preco_max_global = float(df["preco_venda"].max(skipna=True) or 1)
preco_range = st.sidebar.slider(
    "Preço (R$)",
    min_value=0.0,
    max_value=preco_max_global,
    value=(0.0, preco_max_global),
    step=10000.0,
    format="R$ %.0f",
)

desconto_min = st.sidebar.slider("Desconto mínimo (%)", 0, 90, 0, step=5)

modalidades_disponiveis = sorted(df["modalidade"].dropna().unique().tolist())
modalidades = st.sidebar.multiselect("Modalidade", modalidades_disponiveis, default=[])

financiamento = st.sidebar.selectbox("Aceita financiamento?", ["Qualquer", "Sim", "Não"])

quartos_min = st.sidebar.selectbox("Quartos (mínimo)", ["Qualquer", "1", "2", "3", "4+"], index=0)


# ---------- Aplicar filtros --------------------------------------------------

f = df.copy()
if ufs:
    f = f[f["uf"].isin(ufs)]
if cidades:
    f = f[f["cidade"].isin(cidades)]
if tipos:
    f = f[f["tipo_imovel"].isin(tipos)]
f = f[(f["preco_venda"].fillna(0) >= preco_range[0]) & (f["preco_venda"].fillna(0) <= preco_range[1])]
if desconto_min > 0:
    f = f[f["desconto_pct"].fillna(0) >= desconto_min]
if modalidades:
    f = f[f["modalidade"].isin(modalidades)]
if financiamento != "Qualquer":
    f = f[f["aceita_financiamento"] == financiamento]
if quartos_min != "Qualquer":
    n = 4 if quartos_min == "4+" else int(quartos_min)
    f = f[f["quartos"].fillna(0) >= n]


# ---------- Header & KPIs ----------------------------------------------------

st.title("🏠 Leilões Caixa Econômica Federal")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Imóveis", f"{len(f):,}".replace(",", "."))
c2.metric("Preço mediano", f"R$ {f['preco_venda'].median():,.0f}".replace(",", ".") if f['preco_venda'].notna().any() else "—")
c3.metric("Desconto mediano", f"{f['desconto_pct'].median():.1f}%" if f['desconto_pct'].notna().any() else "—")
c4.metric("Cidades", f["cidade"].nunique())


# ---------- Tabs -------------------------------------------------------------

tab_lista, tab_cidades, tab_stats = st.tabs(["📋 Lista", "🏙️ Por cidade", "📊 Estatísticas"])

with tab_lista:
    sort_col = st.selectbox(
        "Ordenar por",
        ["desconto_pct", "preco_venda", "valor_avaliacao", "area_m2"],
        format_func=lambda x: {
            "desconto_pct": "Maior desconto",
            "preco_venda": "Menor preço",
            "valor_avaliacao": "Maior avaliação",
            "area_m2": "Maior área",
        }[x],
    )
    asc = sort_col == "preco_venda"
    f_sorted = f.sort_values(sort_col, ascending=asc, na_position="last")

    cols_display = [
        "uf", "cidade", "bairro", "tipo_imovel", "quartos", "area_m2",
        "preco_venda", "valor_avaliacao", "desconto_pct", "modalidade",
        "endereco", "url_detalhe",
    ]
    cols_display = [c for c in cols_display if c in f_sorted.columns]

    st.dataframe(
        f_sorted[cols_display].head(500),
        use_container_width=True,
        hide_index=True,
        column_config={
            "preco_venda": st.column_config.NumberColumn("Preço (R$)", format="R$ %.0f"),
            "valor_avaliacao": st.column_config.NumberColumn("Avaliação (R$)", format="R$ %.0f"),
            "desconto_pct": st.column_config.NumberColumn("Desconto", format="%.1f%%"),
            "area_m2": st.column_config.NumberColumn("Área (m²)", format="%.1f"),
            "url_detalhe": st.column_config.LinkColumn("Caixa", display_text="🔗"),
        },
    )

    if len(f_sorted) > 500:
        st.info(f"A mostrar 500 de {len(f_sorted)} imóveis. Aperta os filtros para ver os restantes.")

    # Drill-down
    st.divider()
    st.subheader("🔬 Drill-down — detalhes de um imóvel")
    ids_disponiveis = f_sorted["id_imovel"].dropna().head(500).tolist()
    if ids_disponiveis:
        chosen_id = st.selectbox("Escolhe um imóvel", ids_disponiveis)
        if chosen_id:
            row = f_sorted[f_sorted["id_imovel"] == chosen_id].iloc[0]
            colA, colB = st.columns([1, 2])
            with colA:
                # As URLs de foto da Caixa precisam de um dígito verificador
                # que não vem no CSV — pelo que mostramos um placeholder bonito
                # e deixamos o utilizador ver as fotos reais no site da Caixa.
                st.markdown(
                    """
                    <div style="
                        background: linear-gradient(135deg, #003B5C 0%, #FF6B35 100%);
                        border-radius: 12px;
                        height: 280px;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        color: white;
                        font-size: 4em;
                        margin-bottom: 1em;
                    ">🏠</div>
                    <p style="text-align: center; color: #666; font-size: 0.9em;">
                        📷 Ver fotos no site oficial da Caixa →
                    </p>
                    """,
                    unsafe_allow_html=True,
                )
            with colB:
                st.markdown(f"### {row.get('cidade', '')} — {row.get('bairro', '')}")
                st.markdown(f"**Endereço:** {row.get('endereco', '—')}")
                st.markdown(f"**Tipo:** {row.get('tipo_imovel', '—')} • **Quartos:** {row.get('quartos', '—')} • **Área:** {row.get('area_m2', '—')} m²")
                preco = row.get('preco_venda')
                aval = row.get('valor_avaliacao')
                desc = row.get('desconto_pct')
                if preco: st.markdown(f"**Preço:** R$ {preco:,.0f}".replace(",", "."))
                if aval: st.markdown(f"**Avaliação:** R$ {aval:,.0f}".replace(",", "."))
                if desc: st.markdown(f"**Desconto:** {desc:.1f}%")
                st.markdown(f"**Modalidade:** {row.get('modalidade', '—')}")
                st.markdown(f"**Financiamento:** {row.get('aceita_financiamento', '—')}")
                st.markdown(f"**Descrição:** {row.get('descricao', '—')}")
                if row.get("url_detalhe"):
                    st.link_button("Ver página oficial Caixa →", row["url_detalhe"])

with tab_cidades:
    st.subheader("Oportunidades agregadas por cidade")
    agg = f.groupby(["uf", "cidade"]).agg(
        n_imoveis=("id_imovel", "count"),
        preco_mediano=("preco_venda", "median"),
        desconto_medio=("desconto_pct", "mean"),
        desconto_max=("desconto_pct", "max"),
    ).reset_index().sort_values("n_imoveis", ascending=False)

    st.dataframe(
        agg.head(200),
        use_container_width=True,
        hide_index=True,
        column_config={
            "preco_mediano": st.column_config.NumberColumn("Preço mediano", format="R$ %.0f"),
            "desconto_medio": st.column_config.NumberColumn("Desconto médio", format="%.1f%%"),
            "desconto_max": st.column_config.NumberColumn("Desconto máx", format="%.1f%%"),
        },
    )

with tab_stats:
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Imóveis por estado**")
        st.bar_chart(f.groupby("uf").size().sort_values(ascending=False))
    with c2:
        st.markdown("**Imóveis por tipo**")
        st.bar_chart(f.groupby("tipo_imovel").size().sort_values(ascending=False))

    st.markdown("**Distribuição de descontos**")
    st.bar_chart(f["desconto_pct"].dropna(), x_label="Desconto (%)")
