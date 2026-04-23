"""
Dashboard de leilões da Caixa Econômica Federal.

Run local: streamlit run app.py
"""

from __future__ import annotations

from pathlib import Path

import folium
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

DATA_FILE = Path(__file__).parent / "data" / "imoveis.parquet"

st.set_page_config(
    page_title="Leilões Caixa",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------- Carregamento de dados (cached) -----------------------------------

@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    if not DATA_FILE.exists():
        return pd.DataFrame()
    df = pd.read_parquet(DATA_FILE)
    # Colunas que podem não existir se o scraper ainda não correu o geocoder
    for col in ["lat", "lon"]:
        if col not in df.columns:
            df[col] = None
    return df


df = load_data()

if df.empty:
    st.error(
        "Não há dados ainda. Corre primeiro:\n\n"
        "```\npython scraper.py\npython geocoder.py\n```"
    )
    st.stop()


# ---------- Sidebar: filtros -------------------------------------------------

st.sidebar.title("🔍 Filtros")

ufs_disponiveis = sorted(df["uf"].dropna().unique().tolist())
ufs = st.sidebar.multiselect("Estado (UF)", ufs_disponiveis, default=[])

# Cidade depende da UF selecionada
if ufs:
    cidades_disponiveis = sorted(df[df["uf"].isin(ufs)]["cidade"].dropna().unique().tolist())
else:
    cidades_disponiveis = sorted(df["cidade"].dropna().unique().tolist())

cidades = st.sidebar.multiselect("Cidade", cidades_disponiveis, default=[])

tipos_disponiveis = sorted(df["tipo_imovel"].dropna().unique().tolist())
tipos = st.sidebar.multiselect("Tipo de imóvel", tipos_disponiveis, default=tipos_disponiveis)

# Faixa de preço
preco_min, preco_max = float(df["preco_venda"].min(skipna=True) or 0), float(df["preco_venda"].max(skipna=True) or 1)
preco_range = st.sidebar.slider(
    "Preço de venda (R$)",
    min_value=0.0,
    max_value=preco_max,
    value=(0.0, preco_max),
    step=10000.0,
    format="R$ %.0f",
)

# Faixa de desconto
desconto_min = st.sidebar.slider("Desconto mínimo (%)", 0, 90, 0, step=5)

# Modalidade
modalidades_disponiveis = sorted(df["modalidade"].dropna().unique().tolist())
modalidades = st.sidebar.multiselect("Modalidade", modalidades_disponiveis, default=[])

# Quartos mínimos
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
if quartos_min != "Qualquer":
    n = 4 if quartos_min == "4+" else int(quartos_min)
    f = f[f["quartos"].fillna(0) >= n]


# ---------- Header & KPIs ----------------------------------------------------

st.title("🏠 Leilões Caixa Econômica Federal")
st.caption(f"Última atualização dos dados: {DATA_FILE.stat().st_mtime if DATA_FILE.exists() else 'n/a'}")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Imóveis filtrados", f"{len(f):,}".replace(",", "."))
c2.metric("Preço mediano", f"R$ {f['preco_venda'].median():,.0f}".replace(",", "."))
c3.metric("Desconto mediano", f"{f['desconto_pct'].median():.1f}%")
c4.metric("UFs cobertas", f["uf"].nunique())


# ---------- Tabs: Lista | Mapa | Detalhe -------------------------------------

tab_lista, tab_mapa, tab_stats = st.tabs(["📋 Lista", "🗺️ Mapa", "📊 Estatísticas"])

with tab_lista:
    st.subheader(f"Lista de imóveis ({len(f)})")

    # Ordenação
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
        f_sorted[cols_display].head(500),  # limite por performance
        use_container_width=True,
        hide_index=True,
        column_config={
            "preco_venda": st.column_config.NumberColumn("Preço (R$)", format="R$ %.0f"),
            "valor_avaliacao": st.column_config.NumberColumn("Avaliação (R$)", format="R$ %.0f"),
            "desconto_pct": st.column_config.NumberColumn("Desconto", format="%.1f%%"),
            "area_m2": st.column_config.NumberColumn("Área (m²)", format="%.1f"),
            "url_detalhe": st.column_config.LinkColumn("Ver no site Caixa", display_text="Abrir 🔗"),
        },
    )

    if len(f_sorted) > 500:
        st.info(f"A mostrar 500 de {len(f_sorted)} imóveis. Aperta os filtros para ver os restantes.")

    # Drill-down: seleção por ID
    st.divider()
    st.subheader("🔬 Drill-down — ver detalhes de um imóvel")
    ids_disponiveis = f_sorted["id_imovel"].dropna().head(500).tolist()
    if ids_disponiveis:
        chosen_id = st.selectbox("Escolhe o ID do imóvel", ids_disponiveis)
        if chosen_id:
            row = f_sorted[f_sorted["id_imovel"] == chosen_id].iloc[0]
            colA, colB = st.columns([1, 2])
            with colA:
                if row.get("url_foto"):
                    st.image(row["url_foto"], use_container_width=True)
            with colB:
                st.markdown(f"### {row.get('cidade', '')} — {row.get('bairro', '')}")
                st.markdown(f"**Endereço:** {row.get('endereco', '—')}")
                st.markdown(f"**Tipo:** {row.get('tipo_imovel', '—')} • **Quartos:** {row.get('quartos', '—')} • **Área:** {row.get('area_m2', '—')} m²")
                st.markdown(f"**Preço:** R$ {row.get('preco_venda', 0):,.2f}".replace(",", "."))
                st.markdown(f"**Avaliação:** R$ {row.get('valor_avaliacao', 0):,.2f}".replace(",", "."))
                st.markdown(f"**Desconto:** {row.get('desconto_pct', 0):.1f}%")
                st.markdown(f"**Modalidade:** {row.get('modalidade', '—')}")
                st.markdown(f"**Descrição:** {row.get('descricao', '—')}")
                if row.get("url_detalhe"):
                    st.link_button("Ver página oficial da Caixa →", row["url_detalhe"])

with tab_mapa:
    st.subheader("Mapa interativo")

    com_coords = f.dropna(subset=["lat", "lon"])
    sem_coords = len(f) - len(com_coords)
    if sem_coords:
        st.caption(f"⚠️ {sem_coords} imóveis sem coordenadas (CEP não geocodificado).")

    if com_coords.empty:
        st.info("Não há imóveis com coordenadas para mostrar. Corre o `geocoder.py` primeiro.")
    else:
        # Centra no centroide
        center = [com_coords["lat"].mean(), com_coords["lon"].mean()]
        m = folium.Map(location=center, zoom_start=5, tiles="OpenStreetMap")

        # Limita marcadores para não rebentar o browser
        sample = com_coords.head(2000)
        for _, row in sample.iterrows():
            popup = folium.Popup(
                f"""
                <b>{row.get('cidade', '')} — {row.get('bairro', '')}</b><br>
                <b>R$ {row.get('preco_venda', 0):,.0f}</b> ({row.get('desconto_pct', 0):.0f}% off)<br>
                {row.get('tipo_imovel', '')} • {row.get('quartos', '?')} quartos<br>
                <a href="{row.get('url_detalhe', '#')}" target="_blank">Ver no site Caixa</a>
                """.replace(",", "."),
                max_width=300,
            )
            folium.CircleMarker(
                location=[row["lat"], row["lon"]],
                radius=5,
                popup=popup,
                color="#003B5C",
                fill=True,
                fillColor="#FF6B35",
                fillOpacity=0.7,
            ).add_to(m)

        st_folium(m, use_container_width=True, height=600, returned_objects=[])
        if len(com_coords) > 2000:
            st.caption(f"A mostrar 2000 de {len(com_coords)} imóveis. Aperta os filtros para granularidade maior.")

with tab_stats:
    st.subheader("Distribuição de oportunidades")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Imóveis por estado**")
        por_uf = f.groupby("uf").size().sort_values(ascending=False)
        st.bar_chart(por_uf)
    with c2:
        st.markdown("**Imóveis por tipo**")
        por_tipo = f.groupby("tipo_imovel").size().sort_values(ascending=False)
        st.bar_chart(por_tipo)

    st.markdown("**Histograma de preços**")
    st.bar_chart(f["preco_venda"].dropna().clip(upper=2_000_000), x_label="Preço (R$, capped @ 2M)")

    st.markdown("**Histograma de descontos**")
    st.bar_chart(f["desconto_pct"].dropna(), x_label="Desconto (%)")
