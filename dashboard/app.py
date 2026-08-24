import streamlit as st
import plotly.express as px

from analytics.gold_data import (
    carregar_onibus_por_dia_tipo,
    carregar_atraso_por_linha,
)

# paleta validada (dataviz skill, references/palette.md) — slots 1/2 do
# categórico (azul/laranja), superfície e tinta do tema claro
AZUL = "#2a78d6"
LARANJA = "#eb6834"
SUPERFICIE = "#fcfcfb"
TINTA_PRIMARIA = "#0b0b0b"

MIN_VIAGENS_KPI = 5

st.set_page_config(page_title="SafeMoove", layout="wide")
st.title("SafeMoove — Operação de ônibus, São Paulo")


@st.cache_data(ttl=600)
def carregar_dados():
    return carregar_onibus_por_dia_tipo(), carregar_atraso_por_linha()


def aplicar_tema(fig):
    fig.update_layout(plot_bgcolor=SUPERFICIE, paper_bgcolor=SUPERFICIE, font_color=TINTA_PRIMARIA)
    return fig


onibus, atraso = carregar_dados()

if onibus.empty or atraso.empty:
    st.warning("Sem dado nas tabelas gold ainda — rode o pipeline e a Lambda de refresh antes.")
    st.stop()

# --- KPIs ---
col1, col2, col3 = st.columns(3)

ultimo_dia = onibus["dia_circulacao"].max()
total_onibus_hoje = onibus.loc[onibus["dia_circulacao"] == ultimo_dia, "qtd_onibus"].sum()
col1.metric(f"Ônibus distintos (dia {ultimo_dia})", f"{total_onibus_hoje:,}")

col2.metric("Atraso médio geral (min)", f"{atraso['atraso_medio_minutos'].mean():.1f}")

confiaveis = atraso[atraso["qtd_viagens"] >= MIN_VIAGENS_KPI]
if not confiaveis.empty:
    pior = confiaveis.loc[confiaveis["atraso_medio_minutos"].idxmax()]
    col3.metric("Linha mais atrasada", f"{pior['letreiro']}", f"{pior['atraso_medio_minutos']:.0f} min")
else:
    col3.metric("Linha mais atrasada", "sem dado suficiente")

st.divider()

# --- Ônibus por tipo de linha, por dia ---
st.subheader("Ônibus por tipo de linha")

# mapeamento confirmado na documentação oficial da SPTrans (tl = BASE/ATENDIMENTO);
# códigos fora dessa lista não são documentados publicamente, mantidos como estão
MAPA_TIPO_LINHA = {
    10: "principal",
    21: "atendimento",
    23: "atendimento",
    32: "atendimento",
    41: "atendimento",
}
onibus["tipo_linha"] = onibus["tipo_linha"].map(MAPA_TIPO_LINHA).fillna(onibus["tipo_linha"].astype(str))

fig_onibus = px.bar(
    onibus.sort_values("tipo_linha"),
    x="tipo_linha",
    y="qtd_onibus",
    color="dia_circulacao",
    barmode="group",
    color_discrete_sequence=[AZUL, LARANJA],
    labels={"tipo_linha": "Tipo de linha", "qtd_onibus": "Ônibus distintos", "dia_circulacao": "Dia"},
)
st.plotly_chart(aplicar_tema(fig_onibus), use_container_width=True)

st.divider()

# --- Atraso por linha ---
st.subheader("Linhas com maior atraso")

dias_disponiveis = sorted(atraso["dia_circulacao"].unique(), reverse=True)
col_a, col_b = st.columns(2)
dia_selecionado = col_a.selectbox("Dia", dias_disponiveis)
qtd_minima = col_b.slider(
    "Mínimo de viagens observadas (confiabilidade da estimativa)",
    min_value=1,
    max_value=int(atraso["qtd_viagens"].max()),
    value=MIN_VIAGENS_KPI,
)

filtrado = atraso[(atraso["dia_circulacao"] == dia_selecionado) & (atraso["qtd_viagens"] >= qtd_minima)]
top20 = filtrado.sort_values("atraso_medio_minutos", ascending=False).head(20)

if top20.empty:
    st.info("Nenhuma linha atende esse filtro nesse dia.")
else:
    fig_atraso = px.bar(
        top20.sort_values("atraso_medio_minutos"),
        x="atraso_medio_minutos",
        y="letreiro",
        orientation="h",
        color_discrete_sequence=[AZUL],
        hover_data={"origem": True, "destino": True, "qtd_viagens": True, "letreiro": False},
        labels={"atraso_medio_minutos": "Atraso médio (min)", "letreiro": "Linha"},
    )
    st.plotly_chart(aplicar_tema(fig_atraso), use_container_width=True)

st.caption(
    "Atraso é uma estimativa mínima observada (última leitura em que o veículo ainda "
    "aparecia \"a caminho\" menos a primeira previsão da viagem), não o valor exato — "
    "ver Limitações no README."
)
