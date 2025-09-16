# app/app.py
import os, json, requests
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="NYC Yellow Taxi — Jun–Jul 2025", layout="wide")

# ========= CONFIG =========
# Ajuste via Secrets (Streamlit Cloud) ou deixe os defaults:
BUCKET = os.getenv("BUCKET", "nyc-taxi-portfolio-frm")
PREFIX = os.getenv("PREFIX", "agg")
S3_PATH = f"s3://{BUCKET}/{PREFIX}"

# Se houver AWS keys (via Secrets), usa-as; senão tenta leitura anônima
_has_keys = bool(os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
STORAGE_OPTS: dict = {} if _has_keys else {"anon": True}
if _has_keys and os.getenv("AWS_DEFAULT_REGION"):
    STORAGE_OPTS["client_kwargs"] = {"region_name": os.getenv("AWS_DEFAULT_REGION")}

# ========= HELPERS =========
GEOJSON_URL = (
    "https://data.cityofnewyork.us/api/geospatial/8meu-9t5y?method=export&format=GeoJSON"
)  # NYC Taxi Zones (GeoJSON) — NYC Open Data

def load_taxi_geojson():
    """
    1) tenta 'data/NYC Taxi Zones.geojson'
    2) tenta 'data/taxi_zones.geojson'
    3) fallback: baixa GeoJSON oficial do NYC Open Data
    """
    candidates = ["data/NYC Taxi Zones.geojson", "data/taxi_zones.geojson"]
    for path in candidates:
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
    # Lê um diretório de Parquets (CTAS do Athena) no S3
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
    st.info(
        "Se o bucket for privado, preencha em Settings → Secrets: "
        "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION "
        "além de (opcional) BUCKET/PREFIX. "
        "Alternativa: torne público somente o prefixo agg/*."
    )
    st.stop()

# sanity e tipos
guard_df(daily,   "agg_daily")
guard_df(hourdow, "agg_hour_dow")
guard_df(zonepu,  "agg_zone_pickup")
guard_df(pay,     "agg_payment")
daily["pickup_date"] = pd.to_datetime(daily["pickup_date"])
pay["pickup_date"]   = pd.to_datetime(pay["pickup_date"])

# ========= GEOJSON =========
taxi_gj = load_taxi_geojson()

# ========= UI / FILTERS =========
st.title("🚕 NYC Yellow Taxi — Jun–Jul 2025")
st.caption(
    "Fonte: NYC TLC Trip Record Data (Parquet) • Pré-agregações por Athena CTAS • "
    "Mapa: NYC Taxi Zones (GeoJSON)."
)

min_d, max_d = daily["pickup_date"].min().date(), daily["pickup_date"].max().date()
c1, c2 = st.columns([2, 1])
dr = c1.date_input("Período", [min_d, max_d], min_value=min_d, max_value=max_d)
hr_min, hr_max = c2.select_slider("Hora (pickup)", options=list(range(24)), value=(0, 23))

d0, d1 = pd.to_datetime(dr[0]), pd.to_datetime(dr[1])
daily_f   = daily[(daily.pickup_date >= d0) & (daily.pickup_date <= d1)]
hourdow_f = hourdow[(hourdow.pickup_hour >= hr_min) & (hourdow.pickup_hour <= hr_max)]
zonepu_f  = zonepu.copy()  # (agg de zonas já vem totalizado p/ jun–jul)
pay_f     = pay[(pay.pickup_date >= d0) & (pay.pickup_date <= d1)]

# ========= KPIs =========
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Viagens", f"{int(daily_f['trips'].sum()):,}")
k2.metric("Receita ($)", f"{daily_f['revenue_total'].sum():,.0f}")
k3.metric("Tarifa média ($)", f"{daily_f['avg_fare'].mean():.2f}")
k4.metric("Tip % médio", f"{100 * daily_f['avg_tip_pct'].mean():.1f}%")
k5.metric("Distância média (mi)", f"{daily_f['avg_trip_miles'].mean():.2f}")

# ========= CHARTS =========
# Série diária
st.plotly_chart(
    px.line(daily_f, x="pickup_date", y="trips", title="Viagens por dia"),
    use_container_width=True,
)

# Heatmap hora × dia-da-semana
heat = hourdow_f.pivot_table(index="pickup_dow_num", columns="pickup_hour",
                             values="trips", aggfunc="sum").fillna(0)
st.plotly_chart(
    px.imshow(heat, aspect="auto", title="Heatmap (dia da semana × hora)"),
    use_container_width=True,
)

# Ranking de zonas (top 15 por trips)
top = (
    zonepu_f.groupby(["borough", "zone"], as_index=False)
    .agg(trips=("trips", "sum"), revenue=("revenue_total", "sum"))
    .sort_values("trips", ascending=False)
    .head(15)
)
st.dataframe(top, use_container_width=True)

# Mapa por zona (match por nome: properties.zone)
zone_counts = zonepu_f.groupby("zone", as_index=False)["trips"].sum()
fig = px.choropleth_mapbox(
    zone_counts,
    geojson=taxi_gj,
    locations="zone",
    featureidkey="properties.zone",
    color="trips",
    mapbox_style="open-street-map",  # sem token
    zoom=9,
    center={"lat": 40.7128, "lon": -74.0060},
    opacity=0.6,
    title="Pickups por zona (jun–jul/2025)",
)
st.plotly_chart(fig, use_container_width=True)

st.caption(
    "CTAS grava Parquet diretamente no S3 (external_location). "
    "Se precisar casar ID↔zona/borough, use o Taxi Zone Lookup CSV do TLC."
)
