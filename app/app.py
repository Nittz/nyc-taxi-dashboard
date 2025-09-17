# app/app.py
import os, json, requests
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="NYC Yellow Taxi — Jun–Jul 2025", layout="wide")

# ========= CONFIG =========
BUCKET = os.getenv("BUCKET", "nyc-taxi-portfolio-frm")
PREFIX = os.getenv("PREFIX", "agg_v3")  # mantém agg_v3 (underscore)
S3_PATH = f"s3://{BUCKET}/{PREFIX}"

# 🔧 IMPORTANTE: nunca use anônimo aqui; deixe o boto3 achar credenciais (role/perfil/secrets)
STORAGE_OPTS: dict = {"anon": False}
# (Opcional) Se você quiser forçar chaves explícitas via env/secrets, mantemos compatível:
if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
    STORAGE_OPTS.update({
        "key": os.getenv("AWS_ACCESS_KEY_ID"),
        "secret": os.getenv("AWS_SECRET_ACCESS_KEY")
    })
if os.getenv("AWS_SESSION_TOKEN"):
    STORAGE_OPTS["token"] = os.getenv("AWS_SESSION_TOKEN")
if os.getenv("AWS_DEFAULT_REGION"):
    STORAGE_OPTS["client_kwargs"] = {"region_name": os.getenv("AWS_DEFAULT_REGION")}

# ========= HELPERS =========
GEOJSON_URL = "https://data.cityofnewyork.us/api/geospatial/8meu-9t5y?method=export&format=GeoJSON"

def load_taxi_geojson():
    for path in ["data/NYC Taxi Zones.geojson", "data/taxi_zones.geojson"]:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    gj = json.load(f)
                if isinstance(gj, dict) and gj.get("type") == "FeatureCollection":
                    return gj
            except Exception:
                pass
    r = requests.get(GEOJSON_URL, timeout=30)
    r.raise_for_status()
    return r.json()

@st.cache_data(show_spinner=False)
def read_parquet_dir(path: str) -> pd.DataFrame:
    return pd.read_parquet(path, storage_options=STORAGE_OPTS)

def guard_df(df: pd.DataFrame, name: str):
    if df is None or len(df) == 0:
        st.error(f"Nenhum dado em {name}. Confira no S3: {S3_PATH}/{name}/")
        st.stop()

# ========= LOAD DATA =========
try:
    daily   = read_parquet_dir(f"{S3_PATH}/agg_daily/")
    hourdow = read_parquet_dir(f"{S3_PATH}/agg_hour_dow/")
    zonepu  = read_parquet_dir(f"{S3_PATH}/agg_zone_pickup/")
    pay     = read_parquet_dir(f"{S3_PATH}/agg_payment/")
except Exception as e:
    st.error(f"Erro ao ler Parquet no S3 ({S3_PATH}). Detalhe: {e}")
    st.stop()

guard_df(daily,   "agg_daily")
guard_df(hourdow, "agg_hour_dow")
guard_df(zonepu,  "agg_zone_pickup")
guard_df(pay,     "agg_payment")

# Tipos
daily["pickup_date"] = pd.to_datetime(daily["pickup_date"])
pay["pickup_date"]   = pd.to_datetime(pay["pickup_date"])

# ========= GEOJSON =========
taxi_gj = load_taxi_geojson()

# ========= UI / FILTERS =========
st.title("🚕 NYC Yellow Taxi — Jun–Jul 2025")
st.caption(
    "Fonte: NYC TLC Trip Record Data (Parquet) • Pré-agregações (Athena) • "
    "Filtro de HORA aplicado globalmente por ponderação Hora×DOW."
)

min_d, max_d = daily["pickup_date"].min().date(), daily["pickup_date"].max().date()
c1, c2 = st.columns([2, 1])
dr = c1.date_input("Período", [min_d, max_d], min_value=min_d, max_value=max_d)
hr_min, hr_max = c2.select_slider("Hora (pickup)", options=list(range(24)), value=(0, 23))

# ========= PROPAGAÇÃO DO FILTRO DE HORA =========
# 1) Fração por DOW dentro do intervalo de horas selecionadas (base: agg_hour_dow)
assert {"pickup_dow_num", "pickup_hour", "trips"}.issubset(hourdow.columns), "agg_hour_dow com colunas inesperadas."
dow_tot = hourdow.groupby("pickup_dow_num", as_index=True)["trips"].sum()
dow_sel = (
    hourdow[(hourdow["pickup_hour"] >= hr_min) & (hourdow["pickup_hour"] <= hr_max)]
    .groupby("pickup_dow_num", as_index=True)["trips"].sum()
)
ratio_by_dow = (dow_sel / dow_tot).fillna(0).clip(0, 1)  # Series index 1..7

# 2) Série diária/KPIs: aplica a razão conforme o DOW de cada data
daily_f = daily[(daily.pickup_date >= pd.to_datetime(dr[0])) & (daily.pickup_date <= pd.to_datetime(dr[1]))].copy()
daily_f["pickup_dow_num"] = daily_f["pickup_date"].dt.dayofweek + 1
daily_f["__ratio"] = daily_f["pickup_dow_num"].map(ratio_by_dow).fillna(0)

for col in ["trips", "revenue_total", "fare_sum", "tip_sum", "distance_sum"]:
    if col in daily_f.columns:
        daily_f[f"{col}__hr"] = daily_f[col] * daily_f["__ratio"]

trips_total   = int(daily_f.get("trips__hr", daily_f.get("trips", pd.Series(dtype=float))).sum())
revenue_total = float(daily_f.get("revenue_total__hr", daily_f.get("revenue_total", pd.Series(dtype=float))).sum())
fare_sum      = float(daily_f.get("fare_sum__hr", pd.Series(dtype=float)).sum())
tip_sum       = float(daily_f.get("tip_sum__hr", pd.Series(dtype=float)).sum())
dist_sum      = float(daily_f.get("distance_sum__hr", pd.Series(dtype=float)).sum())
def safe_div(a, b): return (a / b) if (b and b != 0) else 0.0
avg_fare    = safe_div(fare_sum, trips_total) if fare_sum else (float(daily_f.get("avg_fare", pd.Series([0])).mean()) if trips_total else 0.0)
avg_tip_pct = safe_div(tip_sum, fare_sum) if fare_sum else (float(daily_f.get("avg_tip_pct", pd.Series([0])).mean()))
avg_miles   = safe_div(dist_sum, trips_total) if dist_sum else (float(daily_f.get("avg_trip_miles", pd.Series([0])).mean()) if trips_total else 0.0)

# 3) Pagamentos: pondera por DOW da data
pay_f = pay[(pay.pickup_date >= pd.to_datetime(dr[0])) & (pay.pickup_date <= pd.to_datetime(dr[1]))].copy()
if not pay_f.empty:
    pay_f["pickup_dow_num"] = pay_f["pickup_date"].dt.dayofweek + 1
    pay_f["__ratio"] = pay_f["pickup_dow_num"].map(ratio_by_dow).fillna(0)
    for col in ["trips", "revenue_total", "fare_sum", "tip_sum"]:
        if col in pay_f.columns:
            pay_f[f"{col}__hr"] = pay_f[col] * pay_f["__ratio"]

# 4) Zonas (totalizadas no período): aplica fator global (aproximação)
sel_all = hourdow[(hourdow["pickup_hour"] >= hr_min) & (hourdow["pickup_hour"] <= hr_max)]["trips"].sum()
tot_all = hourdow["trips"].sum()
global_ratio = float(sel_all / tot_all) if tot_all else 0.0
zonepu_scaled = zonepu.copy()
if "trips" in zonepu_scaled.columns:
    zonepu_scaled["trips"] = zonepu_scaled["trips"] * global_ratio
if "revenue_total" in zonepu_scaled.columns:
    zonepu_scaled["revenue_total"] = zonepu_scaled["revenue_total"] * global_ratio

# ========= KPIs =========
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Viagens (horas selecionadas)", f"{trips_total:,}")
k2.metric("Receita ($)", f"{revenue_total:,.0f}")
k3.metric("Tarifa média ($)", f"{avg_fare:.2f}")
k4.metric("Tip % médio", f"{100 * avg_tip_pct:.1f}%")
k5.metric("Distância média (mi)", f"{avg_miles:.2f}")

# ========= CHARTS =========
# Série diária (usa trips__hr se existir)
series_daily = (
    daily_f
    .assign(trips_plot=daily_f["trips__hr"] if "trips__hr" in daily_f.columns else daily_f["trips"])
    .groupby("pickup_date", as_index=False)["trips_plot"].sum()
    .sort_values("pickup_date")
)
st.plotly_chart(
    px.line(series_daily, x="pickup_date", y="trips_plot", title="Viagens por dia (filtrado por hora via DOW)"),
    use_container_width=True,
)

# Heatmap hora × dia-da-semana (exato — vem de agg_hour_dow)
heat = (
    hourdow[(hourdow["pickup_hour"] >= hr_min) & (hourdow["pickup_hour"] <= hr_max)]
    .groupby(["pickup_dow_num", "pickup_hour"], as_index=False)["trips"].sum()
    .pivot(index="pickup_dow_num", columns="pickup_hour", values="trips")
    .fillna(0)
)
st.plotly_chart(
    px.imshow(heat, aspect="auto", title="Heatmap (dia da semana × hora)"),
    use_container_width=True,
)

# Ranking de zonas (escala global — ordem não muda sem base por hora/zone)
top = (
    zonepu_scaled.groupby(["borough", "zone"], as_index=False)
    .agg(trips=("trips", "sum"),
         revenue=("revenue_total", "sum") if "revenue_total" in zonepu_scaled.columns else ("trips","sum"))
    .sort_values("trips", ascending=False)
    .head(15)
)
st.dataframe(top, use_container_width=True)

# Mapa por zona (escala global — cor reage ao filtro de hora)
taxi_gj = load_taxi_geojson()
zone_counts = zonepu_scaled.groupby("zone", as_index=False)["trips"].sum()
fig = px.choropleth_mapbox(
    zone_counts,
    geojson=taxi_gj,
    locations="zone",
    featureidkey="properties.zone",
    color="trips",
    mapbox_style="open-street-map",
    zoom=9,
    center={"lat": 40.7128, "lon": -74.0060},
    opacity=0.6,
    title="Pickups por zona (filtrado por hora — aproximação global)",
)
st.plotly_chart(fig, use_container_width=True)

st.caption(
    "Filtro de hora aplicado globalmente por ponderação Hora×DOW. "
    "Para ranking de zonas variar com a hora, é preciso base por hora por zona (ou CTAS 'agg_zone_pickup_hour_v3')."
)
