# --- imports (já devem existir no seu app)
import os, json
import pandas as pd
import plotly.express as px
import streamlit as st

# --- secrets / S3
BUCKET = st.secrets.get("BUCKET", "nyc-taxi-portfolio-frm")
PREFIX = st.secrets.get("PREFIX", "agg_v3")  # agora apontando pro v3
STORAGE_OPTS = {
    "key":     st.secrets.get("AWS_ACCESS_KEY_ID"),
    "secret":  st.secrets.get("AWS_SECRET_ACCESS_KEY"),
    "client_kwargs": {"region_name": st.secrets.get("AWS_DEFAULT_REGION", "us-east-1")},
}
S3 = f"s3://{BUCKET}/{PREFIX}"

# --- cache de leitura: baixa uma vez por execução/inputs e reutiliza
@st.cache_data(show_spinner=False)
def load_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path, storage_options=STORAGE_OPTS)
# (st.cache_data é o decorador recomendado para funções que retornam dados; acelera o app) :contentReference[oaicite:5]{index=5}

# --- carrega os agregados por HORA (v3)
daily_h = load_parquet(f"{S3}/agg_daily_hour/")
zone_h  = load_parquet(f"{S3}/agg_zone_pickup_hour/")
pay_h   = load_parquet(f"{S3}/agg_payment_hour/")

# --- filtros globais: datas + HORA
dmin, dmax = pd.to_datetime("2025-06-01"), pd.to_datetime("2025-07-31")
sel_date = st.slider("Período", min_value=dmin, max_value=dmax, value=(dmin, dmax))
hmin, hmax = st.slider("Hora (pickup)", 0, 23, (0, 23))  # range slider de hora (0–23) :contentReference[oaicite:6]{index=6}

# aplica os filtros em TODOS os dataframes
mask_d = (daily_h["pickup_date"] >= sel_date[0]) & (daily_h["pickup_date"] <= sel_date[1])
mask_h = (daily_h["pickup_hour"] >= hmin) & (daily_h["pickup_hour"] <= hmax)
dsel = daily_h.loc[mask_d & mask_h].copy()

mask_z = (zone_h["pickup_hour"] >= hmin) & (zone_h["pickup_hour"] <= hmax)
zone_sel = zone_h.loc[mask_z].copy()
zone_sel = zone_sel[(zone_sel["year"]==2025) & (zone_sel["month"].isin([6,7])) &
                    (zone_sel["borough"].notna()) & (zone_sel["zone"].notna())]

mask_p = (pay_h["pickup_date"] >= sel_date[0]) & (pay_h["pickup_date"] <= sel_date[1]) & \
         (pay_h["pickup_hour"] >= hmin) & (pay_h["pickup_hour"] <= hmax)
pay_sel = pay_h.loc[mask_p].copy()

# --- KPIs (médias ponderadas corretas)
trips_total   = int(dsel["trips"].sum())
revenue_total = float(dsel["total_sum"].sum())
fare_sum      = float(dsel["fare_sum"].sum())
tip_sum       = float(dsel["tip_sum"].sum())
dist_sum      = float(dsel["distance_sum"].sum())

avg_fare    = (fare_sum / trips_total) if trips_total else 0.0
avg_tip_pct = (tip_sum / fare_sum) if fare_sum else 0.0
avg_miles   = (dist_sum / trips_total) if trips_total else 0.0

# --- Série diária (1 ponto por dia)
series_daily = (dsel.groupby("pickup_date", as_index=False)
                    .agg(trips=("trips","sum"))
                    .sort_values("pickup_date"))
fig_daily = px.line(series_daily, x="pickup_date", y="trips", markers=True,
                    title="Viagens por dia (filtrado por hora)")
st.plotly_chart(fig_daily, use_container_width=True)

# --- Heatmap hora × dia da semana (a partir dos próprios dsel)
# OBS: day_of_week() no Athena/Trino retorna 1..7; aqui no Python vamos recalcular
dsel["pickup_dow_num"] = pd.to_datetime(dsel["pickup_date"]).dt.dayofweek + 1  # 1=seg..7=dom (coerente com Athena) :contentReference[oaicite:7]{index=7}
heat = (dsel.groupby(["pickup_hour","pickup_dow_num"], as_index=False)
            .agg(trips=("trips","sum")))
fig_heat = px.density_heatmap(heat, x="pickup_hour", y="pickup_dow_num", z="trips",
                              title="Hora × Dia da semana (trips)")
st.plotly_chart(fig_heat, use_container_width=True)

# --- Ranking por zona (agrega com filtro de hora)
top_zones = (zone_sel.groupby(["borough","zone"], as_index=False)
                    .agg(trips=("trips","sum"),
                         revenue_total=("total_sum","sum"))
                    .sort_values("trips", ascending=False)
                    .head(20))

# --- Mapa (choropleth) com GeoJSON: certifique-se de carregar taxi_zones (arquivo local 'data/NYC Taxi Zones.geojson')
# a chave de match depende do seu geojson (ex.: 'properties.zone' ou 'properties.LocationID'):
with open("data/NYC Taxi Zones.geojson", "r") as f:
    taxi_gj = json.load(f)

mapdf = (zone_sel.groupby(["zone"], as_index=False)
                 .agg(trips=("trips","sum")))
fig_map = px.choropleth_mapbox(
    mapdf,
    geojson=taxi_gj,
    locations="zone",
    color="trips",
    featureidkey="properties.zone",  # se o seu geojson usa LocationID, troque para 'properties.LocationID' e mude "locations" também
    mapbox_style="carto-positron",
    center={"lat": 40.7128, "lon": -74.0060}, zoom=9,
    opacity=0.6, title="Trips por zona (filtrado por hora)"
)
st.plotly_chart(fig_map, use_container_width=True)  # API do PX para choropleth_mapbox e featureidkey :contentReference[oaicite:8]{index=8}

# --- Payment por dia
pay_series = (pay_sel.groupby(["pickup_date","payment_type"], as_index=False)
                     .agg(trips=("trips","sum")))
fig_pay = px.bar(pay_series, x="pickup_date", y="trips", color="payment_type",
                 title="Trips por forma de pagamento (por dia, filtrado por hora)")
st.plotly_chart(fig_pay, use_container_width=True)
