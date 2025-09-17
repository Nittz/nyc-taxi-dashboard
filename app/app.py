# app.py
import streamlit as st
import pandas as pd
import altair as alt

# Configura a página: título e layout em largura total
st.set_page_config(page_title="NYC Taxi — v3", layout="wide")

# ------------------------------------
# 1) Carregamento e preparo das colunas
# ------------------------------------
@st.cache_data(show_spinner=True)
def load_data():
    # Leitura inicial de um Parquet local (substituir por S3/Athena apenas neste bloco quando necessário).
    raise_if_no_data = False
    try:
        df = pd.read_parquet("agg-v3.parquet")  # caminho local; ajustar se a fonte mudar
    except Exception:
        if raise_if_no_data:
            raise
        # Dataset sintético para a aplicação iniciar e permitir ajustes de layout/UX antes da fonte real
        rng = pd.date_range("2024-01-01", periods=10000, freq="H")
        df = pd.DataFrame({
            "pickup_datetime": rng,
            "vendorid": 1,
            "total_amount": 10
        })

    # Padroniza nome do timestamp para 'pickup_datetime' se vier com rótulos comuns alternativos
    if "pickup_datetime" not in df.columns:
        for c in ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_ts"]:
            if c in df.columns:
                df = df.rename(columns={c: "pickup_datetime"})
                break

    # Converte para datetime e remove registros inválidos
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df = df.dropna(subset=["pickup_datetime"])

    # Caso a origem esteja em UTC e a visualização precise de fuso NYC, usar a linha abaixo:
    # df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], utc=True).dt.tz_convert("America/New_York")

    # Colunas auxiliares para filtros e visuais
    df["pickup_date"] = df["pickup_datetime"].dt.date
    df["pickup_hour"] = df["pickup_datetime"].dt.hour
    df["dow"] = df["pickup_datetime"].dt.day_name()

    # Ordem fixa dos dias para consistência no heatmap
    cats = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    df["dow"] = pd.Categorical(df["dow"], categories=cats, ordered=True)

    return df

df = load_data()

# ------------------------------
# 2) Sidebar com filtros globais
# ------------------------------
st.sidebar.header("Filtros")

# Intervalo de datas derivado do próprio dataset
min_date = pd.to_datetime(df["pickup_date"]).min()
max_date = pd.to_datetime(df["pickup_date"]).max()

date_sel = st.sidebar.date_input(
    "Período",
    value=(min_date.date(), max_date.date()),
    min_value=min_date.date(),
    max_value=max_date.date()
)

# Filtro de horas (0–23); por padrão, todas selecionadas
hours_default = list(range(24))
hours_sel = st.sidebar.multiselect(
    "Horas do dia (0–23)",
    options=hours_default,
    default=hours_default
)

# Filtro de vendor exibido somente se a coluna existir
vendors = sorted(df["vendorid"].dropna().unique().tolist()) if "vendorid" in df.columns else []
vendor_sel = st.sidebar.multiselect(
    "Vendor",
    options=vendors,
    default=vendors
) if vendors else []

# ---------------------------------------------------
# 3) Aplicação única dos filtros (fonte da verdade)
# ---------------------------------------------------
@st.cache_data(show_spinner=False)
def apply_filters(df, date_range, hours, vendors):
    # Centraliza a lógica de filtros; todos os visuais/kpis derivam deste resultado
    d1, d2 = date_range
    df2 = df[(df["pickup_date"] >= d1) & (df["pickup_date"] <= d2)]
    if hours:
        df2 = df2[df2["pickup_hour"].isin(hours)]
    if vendors:
        df2 = df2[df2["vendorid"].isin(vendors)]
    return df2

df_filtered = apply_filters(df, date_sel, hours_sel, vendor_sel)

# Evita métricas/visuais vazios quando nenhuma hora for selecionada
if len(hours_sel) == 0:
    st.warning("Selecione pelo menos uma hora para aplicar o filtro.")
    st.stop()

# -----------------------------------
# 4) KPIs — sempre com base no filtrado
# -----------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Corridas (registros)", value=f"{len(df_filtered):,}")

with col2:
    if "total_amount" in df_filtered.columns:
        st.metric("Receita total", value=f"${df_filtered['total_amount'].sum():,.2f}")
    else:
        st.metric("Receita total", value="—")

with col3:
    daily_trips = df_filtered.groupby("pickup_date", as_index=False).size()
    st.metric("Média diária (corridas)", value=f"{daily_trips['size'].mean():.1f}" if not daily_trips.empty else "—")

st.divider()

# -------------------------------------------------------------------
# 5) Série diária — filtra por hora primeiro e agrega por dia depois
# -------------------------------------------------------------------
daily = (
    df_filtered
    .groupby("pickup_date", as_index=False)
    .agg(trips=("pickup_datetime", "count"),
         total_amount=("total_amount", "sum"))
)

left, right = st.columns(2)

with left:
    chart_trips = (
        alt.Chart(daily)
        .mark_line(point=True)
        .encode(
            x=alt.X("pickup_date:T", title="Dia"),
            y=alt.Y("trips:Q", title="Corridas"),
            tooltip=["pickup_date:T", "trips:Q"]
        )
        .properties(height=300)
    )
    st.altair_chart(chart_trips, use_container_width=True)

with right:
    if "total_amount" in daily.columns:
        chart_rev = (
            alt.Chart(daily)
            .mark_line(point=True)
            .encode(
                x=alt.X("pickup_date:T", title="Dia"),
                y=alt.Y("total_amount:Q", title="Receita"),
                tooltip=["pickup_date:T", "total_amount:Q"]
            )
            .properties(height=300)
        )
        st.altair_chart(chart_rev, use_container_width=True)
    else:
        st.info("Coluna total_amount não encontrada; gráfico de receita não exibido.")

st.divider()

# -----------------------------------------
# 6) Heatmap — intensidade por Hora x Dia
# -----------------------------------------
heat = (
    df_filtered
    .groupby(["dow","pickup_hour"], as_index=False)
    .agg(trips=("pickup_datetime", "count"))
)

heatmap = (
    alt.Chart(heat)
    .mark_rect()
    .encode(
        x=alt.X("pickup_hour:O", title="Hora do dia"),
        y=alt.Y("dow:O", title="Dia da semana"),
        color=alt.Color("trips:Q", title="Corridas"),
        tooltip=["dow:O", "pickup_hour:O", "trips:Q"]
    )
    .properties(height=280)
)
st.altair_chart(heatmap, use_container_width=True)

# ----------------------------------------------------
# 7) Amostra dos dados já filtrados (inspeção rápida)
# ----------------------------------------------------
with st.expander("Ver amostra dos dados filtrados"):
    st.dataframe(df_filtered.head(50), use_container_width=True)
