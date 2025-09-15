# app/app.py
import os, json, requests
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="NYC Yellow Taxi — Jun–Jul 2025", layout="wide")

# ====== CONFIG S3 (ajuste via Secrets no Streamlit Cloud) ======
BUCKET = os.getenv("BUCKET", "nyc-taxi-portfolio-frm")
PREFIX = os.getenv("PREFIX", "agg")
S3_PATH = f"s3://{BUCKET}/{PREFIX}"

# Se houver AWS keys nas env vars (via Secrets), usa-as; senão tenta acesso anônimo
_has_keys = bool(os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
STORAGE_OPTS: dict = {} if _has_keys else {"anon": True}
if _has_keys and os.getenv("AWS_DEFAULT_REGION"):
    STORAGE_OPTS["client_kwargs"] = {"region_name": os.getenv("AWS_DEFAULT_REGION")}

# ====== FUNÇÕES AUXILIARES ======
GEOJSON_URL = "https://data.cityofnewyork.us/api/geospatial/8meu-9t5y?method=export&format=GeoJSON"  # NYC Taxi Zones

def load_taxi_geojson():
    """
    Tenta carregar o GeoJSON local pelo nome novo:
    data/NYC Taxi Zones.geojson
    -> se falhar, tenta data/taxi_zones.geojson
    -> se ainda falhar, baixa do NYC Open Data (GeoJSON oficial).
    """
    candidates = ["data/NYC Taxi Zones.geojson", "data/taxi_zones.geojson"]
    for path in candidates:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                gj = json.load(f)
            if isinstance(gj, dict) and gj.get("type") == "FeatureCollection":
                return gj
    # fallback online
    r = requests.get(GEOJSON_URL, timeout=30)
    r.raise_for_status()
    return r.json()

def read_parquet_or_fail(path):
    try:
        return pd.read_parquet(path, storage_options=STORAGE_OPTS)
    except Exception as e:
        st.error(f"Erro ao ler: {path}\n{e}")
        st.info(
            "Se o bucket for privado, preencha as Secrets no Streamlit: "
            "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION "
            "(e defina S3_ANON=false ou apenas não defina anon)."
        )
        st.stop()

# ====== CARREGA DADOS (Parquet agregados via CTAS no Athena) ======
daily   = read_parquet_or_fail(f"{S3_PATH}/agg_daily/")
hourdow = read_parquet_or_fail(f"{S3_PATH}/agg_hour_dow/")
zonepu  = read_parquet_or_fail(f"{S3_PATH}/agg_zone_pickup/")
pay     = read_parquet_or_fail(f"{S3_PATH}/agg_payment/")

# Garantir tipos de data
daily["pickup_date"] = pd.to_datetime(daily["pickup_date"])

# ====== GEOJSON DAS TAXI ZONES ======
taxi_gj = load_taxi_geojson()

# ====== UI ======
st.title("🚕 NYC Yellow Taxi — Jun–Jul 2025")
st.caption("Fonte: NYC TLC Trip Record Data (Parquet) • Pré-agregações: Athena CTAS • Mapa: NYC Taxi Zones")

# ---- filtros ----
min_d, max_d = daily["pickup_date"].min().date(), daily["pickup_date"].max().date()
c1, c2 = st.columns([2, 1])
dr = c1.date_input("Período", [min_d, max_d], min_value=min_d, max_value=max_d)
hr_min, hr_max = c2.select_slider("Hora (pickup)", options=list(range(24)), value=(0, 23))

d0, d1 = pd.to_datetime(dr[0]), pd.to_datetime(dr[1])
daily_f   = daily[(daily.pickup_date >= d0) & (daily.pickup_date <= d1)]
hourdow_f = hourdow[(hourdow.pickup_hour >= hr_min) & (hourdow.pickup_hour <= hr_max)]
zonepu_f  = zonepu  # (agg por zona está totalizado nos 2 meses; mantido simples)
pay_f     = pay[(pay.pickup_date >= d0) & (pay.pickup_date <= d1)]

# ---- KPIs ----
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Viagens", f"{int(daily_f['trips'].sum()):,}")
k2.metric("Receita ($)", f"{daily_f['revenue_total'].sum():,.0f}")
k3.metric("Tarifa média ($)", f"{daily_f['avg_fare'].mean():.2f}")
k4.metric("Tip % médio", f"{100 * daily_f['avg_tip_pct'].mean():.1f}%")
k5.metric("Distância média (mi)", f"{daily_f['avg_trip_miles'].mean():.2f}")

# ---- série diária ----
st.plotly_chart(
    px.line(daily_f, x="pickup_date", y="trips", title="Viagens por dia"),
    use_container_width=True,
)

# ---- heatmap hora × DOW ----
heat = hourdow_f.pivot_table(
    index="pickup_dow_num", columns="pickup_hour", values="trips", aggfunc="sum"
).fillna(0)
st.plotly_chart(
    px.imshow(heat, aspect="auto", title="Heatmap (dia da semana × hora)"),
    use_container_width=True,
)

# ---- ranking zonas ----
top = (
    zonepu_f.groupby(["borough", "zone"], as_index=False)
    .agg(trips=("trips", "sum"), revenue=("revenue_total", "sum"))
    .sort_values("trips", ascending=False)
    .head(15)
)
st.dataframe(top, use_container_width=True)

# ---- mapa pickup por zona ----
zone_counts = zonepu_f.groupby("zone", as_index=False)["trips"].sum()
fig = px.choropleth_mapbox(
    zone_counts,
    geojson=taxi_gj,
    locations="zone",
    featureidkey="properties.zone",  # mapeia pelo nome da zona no GeoJSON oficial
    color="trips",
    mapbox_style="carto-positron",
    zoom=9,
    center={"lat": 40.7128, "lon": -74.0060},
    opacity=0.6,
    title="Pickups por zona (jun–jul/2025)",
)
st.plotly_chart(fig, use_container_width=True)

st.caption(
    "Dicas: configure Secrets no Streamlit para buckets privados; se preferir, torne público somente o prefixo agg/* do S3."
)
