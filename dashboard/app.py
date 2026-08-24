from datetime import datetime
from zoneinfo import ZoneInfo

import plotly.express as px
import streamlit as st

from analytics.gold_data import carregar_atraso_por_linha, carregar_onibus_por_dia_tipo

# paleta validada (dataviz skill) contra o fundo bege escolhido -- rodado
# scripts/validate_palette.js "#0f7a9e,#970c4d" --surface #dad2c3: PASS
TEAL = "#0f7a9e"
BORDO = "#970c4d"
SUPERFICIE = "#dad2c3"
TINTA_PRIMARIA = "#363636"
GRADE = "#c9c1b0"
EIXO = "#a89f8c"

MIN_VIAGENS_KPI = 5
TZ_SP = ZoneInfo("America/Sao_Paulo")


@st.cache_data(ttl=600)
def carregar_dados():
    return carregar_onibus_por_dia_tipo(), carregar_atraso_por_linha()


def aplicar_tema(fig, altura=None):
    fig.update_layout(
        plot_bgcolor=SUPERFICIE,
        paper_bgcolor=SUPERFICIE,
        font=dict(color=TINTA_PRIMARIA, size=15),
        legend=dict(font=dict(size=13)),
        title_font=dict(size=17, color=TINTA_PRIMARIA),
        margin=dict(t=50, b=40, l=10, r=10),
    )
    fig.update_xaxes(gridcolor=GRADE, linecolor=EIXO, tickfont=dict(size=13))
    fig.update_yaxes(gridcolor=GRADE, linecolor=EIXO, tickfont=dict(size=13))
    if altura:
        fig.update_layout(height=altura)
    return fig


def render_kpis(onibus, atraso):
    col1, col2, col3 = st.columns(3)

    ultimo_dia = onibus["dia_circulacao"].max()
    total = onibus.loc[onibus["dia_circulacao"] == ultimo_dia, "qtd_onibus"].sum()
    col1.metric(f"🚌 Ônibus distintos (dia {ultimo_dia})", f"{total:,}")

    col2.metric("⏱️ Atraso médio geral", f"{atraso['atraso_medio_minutos'].mean():.1f} min")

    confiaveis = atraso[atraso["qtd_viagens"] >= MIN_VIAGENS_KPI]
    if not confiaveis.empty:
        pior = confiaveis.loc[confiaveis["atraso_medio_minutos"].idxmax()]
        col3.metric("🔴 Linha mais atrasada", pior["letreiro"], f"{pior['atraso_medio_minutos']:.0f} min")
    else:
        col3.metric("🔴 Linha mais atrasada", "sem dado suficiente")


def render_grafico_onibus(onibus):
    fig = px.bar(
        onibus.sort_values("tipo_linha"),
        x="tipo_linha",
        y="qtd_onibus",
        color="dia_circulacao",
        barmode="group",
        text_auto=True,
        color_discrete_sequence=[TEAL, BORDO],
        labels={"tipo_linha": "Tipo de linha", "qtd_onibus": "Ônibus distintos", "dia_circulacao": "Dia"},
        title="Veículos distintos por tipo de linha",
    )
    fig.update_traces(textfont_size=13, textangle=0, textposition="outside", cliponaxis=False)
    st.plotly_chart(aplicar_tema(fig), use_container_width=True)


def render_grafico_atraso(atraso, dia_selecionado, qtd_minima):
    filtrado = atraso[(atraso["dia_circulacao"] == dia_selecionado) & (atraso["qtd_viagens"] >= qtd_minima)]
    top20 = filtrado.sort_values("atraso_medio_minutos", ascending=False).head(20)

    if top20.empty:
        st.info("Nenhuma linha atende esse filtro nesse dia.")
        return

    fig = px.bar(
        top20.sort_values("atraso_medio_minutos"),
        x="atraso_medio_minutos",
        y="letreiro",
        orientation="h",
        text_auto=True,
        color_discrete_sequence=[TEAL],
        hover_data={"origem": True, "destino": True, "qtd_viagens": True, "letreiro": False},
        labels={"atraso_medio_minutos": "Atraso médio (min)", "letreiro": "Linha"},
        title=f"Top {len(top20)} linhas com maior atraso — dia {dia_selecionado}",
    )
    fig.update_traces(textfont_size=12, textposition="outside", cliponaxis=False)
    st.plotly_chart(aplicar_tema(fig, altura=max(320, 28 * len(top20))), use_container_width=True)


def main():
    st.set_page_config(page_title="SafeMoove", page_icon="🚌", layout="wide")

    st.title("🚌 SafeMoove — Operação de ônibus, São Paulo")
    st.caption(
        f"Consultado em {datetime.now(TZ_SP):%d/%m/%Y às %H:%M} (hora local SP) · "
        "dado atualizado automaticamente a cada 30 min"
    )

    onibus, atraso = carregar_dados()

    if onibus.empty or atraso.empty:
        st.warning("Sem dado nas tabelas gold ainda — rode o pipeline e a Lambda de refresh antes.")
        st.stop()

    render_kpis(onibus, atraso)
    st.divider()

    aba_onibus, aba_atraso = st.tabs(["📊 Ônibus por tipo de linha", "⏱️ Atraso por linha"])

    with aba_onibus:
        render_grafico_onibus(onibus)

    with aba_atraso:
        dias_disponiveis = sorted(atraso["dia_circulacao"].unique(), reverse=True)
        col_a, col_b = st.columns(2)
        dia_selecionado = col_a.selectbox("Dia", dias_disponiveis)
        qtd_minima = col_b.slider(
            "Mínimo de viagens observadas (confiabilidade da estimativa)",
            min_value=1,
            max_value=int(atraso["qtd_viagens"].max()),
            value=MIN_VIAGENS_KPI,
        )
        render_grafico_atraso(atraso, dia_selecionado, qtd_minima)

    st.divider()
    st.caption(
        "Atraso é uma estimativa mínima observada (última leitura em que o veículo ainda "
        "aparecia \"a caminho\" menos a primeira previsão da viagem), não o valor exato — "
        "ver Limitações no README."
    )


if __name__ == "__main__":
    main()
