# app/app.py
import os, json, requests
import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import date

st.set_page_config(page_title="NYC Yellow Taxi — Jun–Jul 2025", layout="wide")

# ========= CONFIG =========
# Ajuste via Secrets/Env:
BUCKET = os.getenv("BUCKET", "nyc-taxi-portfolio-frm")
PREFIX = os.getenv("PREFIX", "agg_v3")  # usa agg_v3 como padrão
S3_BASE = f"s3://{BUCKET}/{PREFIX}"
# Subprefixo que contém os dados com timestamp
RAW_SUBPATHS = [
    "yellow_trips_2025/",  # preferido
    "trips/",              # alternativas comumns
    ""
]

# Se houver AWS keys, usa; senão tenta leitura anônima
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
    return pd.read_parquet(path, storage_options=STORAGE_OPTS)

def load_base_with_timestamp() -> pd.DataFrame:
    """
    Carrega a base com timestamp (tpep_pickup_datetime).
    Tenta em ordem os subprefixos definidos em RAW_SUBPATHS.
    """
    last_err = None
    for sub in RAW_SUBPATHS:
        path = f"{S3_BASE}/{sub}" if sub else f"{S3_BASE}/"
        try:
            df = read_parquet_dir(path)
            # Padroniza o nome do carimbo de data/hora
            if "pickup_datetime" not in df.columns:
                for c in ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_ts"]:
                    if c in df.columns:
                        df = df.rename(columns={c: "pickup_datetime"})
                        break
            if "pickup_datetime" not in df.columns:
                raise ValueError("Coluna de timestamp não encontrada.")
            return df
        except Exception as e:
            last_err = e
            continue
    # Se nada deu certo, erra com diagnóstico do último caminho tentado
    raise RuntimeError(f"Falha ao ler base com timestamp em {S3_BASE} (detalhe: {last_err})")

def build_zone_lookup_from_geojson(geojson: dict) -> pd.DataFrame:
    """
    Extrai LocationID, zone e borough do GeoJSON para mostrar nomes no ranking.
    """
    rows = []
    for feat in geojson.get("features", []):
        props = feat.get("properties", {})
        if "LocationID" in props:
            rows.append({
                "pulocationid": int(props["LocationID"]),
                "zone": props.get("zone"),
                "borough": props.get("borough"),
            })
    return pd.DataFrame(rows)

def guard_df(df: pd.DataFrame, name: str):
    if df is None or len(df) == 0:
        st.error(f"Nenhum dado em {name}. Confira no S3: {S3_BASE}")
        st.stop()

# ========= LOAD DATA (AGORA UMA ÚNICA BASE) =========
try:
    base = load_base_with_timestamp()
except Exception as e:
    st.error(f"Erro ao ler Parquet no S3 ({S3_BASE}). Detalhe: {e}")
    st.info(
        "Se o bucket for privado, preencha em Settings → Secrets: "
        "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION "
        "além de (opcional) BUCKET/PREFIX. "
        "Alternativa: torne público o prefixo."
    )
    st.stop()

guard_df(base, "base com timestamp")

# Tipos e colunas auxiliares
base["pickup_datetime"] = pd.to_datetime(base["pickup_datetime"], errors="coerce")
base = base.dropna(subset=["pickup_datetime"]).copy()
base["pickup_date"] = base["pickup_datetime"].dt.date
base["pickup_hour"] = base["pickup_datetime"].dt.hour
# 1..7 (Seg=1 .. Dom=7) para compatibilidade com seu heatmap anterior
base["pickup_dow_num"] = base["pickup_datetime"].dt.dayofweek + 1

# ========= GEOJSON =========
taxi_gj = load_taxi_geojson()
zone_lkp = build_zone_lookup_from_geojson(taxi_gj)

# ========= UI / FILTERS =========
st.title("🚕 NYC Yellow Taxi — Jun–Jul 2025")
st.caption(
    "Fonte: NYC TLC Trip Record Data (Parquet no S3) • Filtro global por data e hora • "
    "Mapa: NYC Taxi Zones (GeoJSON)."
)

min_d, max_d = base["pickup_date"].min(), base["pickup_date"].max()
c1, c2 = st.columns([2, 1])
dr = c1.date_input("Período", [min_d, max_d], min_value=min_d, max_value=max_d)
hr_min, hr_max = c2.select_slider("Hora (pickup)", options=list(range(24)), value=(0, 23))

# Filtro global (aplicado uma única vez)
d0, d1 = pd.to_datetime(dr[0]).date(), pd.to_datetime(dr[1]).date()
mask = (
    (base["pickup_date"] >= d0) &
    (base["pickup_date"] <= d1) &
    (base["pickup_hour"] >= hr_min) &
    (base["pickup_hour"] <= hr_max)
)
df_filtered = base.loc[mask].copy()
guard_df(df_filtered, "df_filtered (após filtros de data+hora)")

# ========= KPIs (derivados do filtrado) =========
# Nem todos os Parquets têm todas as colunas; calcular de forma defensiva
trips_total = int(len(df_filtered))
revenue_total = float(df_filtered["total_amount"].sum()) if "total_amount" in df_filtered.columns else 0.0
fare_sum = float(df_filtered["fare_amount"].sum()) if "fare_amount" in df_filtered.columns else 0.0
tip_sum = float(df_filtered["tip_amount"].sum()) if "tip_amount" in df_filtered.columns else 0.0
dist_sum = float(df_filtered["trip_distance"].sum()) if "trip_distance" in df_filtered.columns else 0.0

avg_fare = (fare_sum / trips_total) if trips_total and fare_sum else 0.0
avg_tip_pct = (tip_sum / fare_sum) if fare_sum else 0.0
avg_miles = (dist_sum / trips_total) if trips_total and dist_sum else 0.0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Viagens", f"{trips_total:,}")
k2.metric("Receita ($)", f"{revenue_total:,.0f}")
k3.metric("Tarifa média ($)", f"{avg_fare:.2f}")
k4.metric("Tip % médio", f"{100 * avg_tip_pct:.1f}%")
k5.metric("Distância média (mi)", f"{avg_miles:.2f}")

# ========= CHARTS (todos a partir do df_filtered) =========
# Série diária
daily = (
    df_filtered.groupby("pickup_date", as_index=False)
    .agg(trips=("pickup_datetime", "count"))
    .sort_values("pickup_date")
)
st.plotly_chart(
    px.line(daily, x="pickup_date", y="trips", title="Viagens por dia"),
    use_container_width=True,
)

# Heatmap hora × dia-da-semana (como antes, mas do filtrado)
heat = (
    df_filtered.groupby(["pickup_dow_num", "pickup_hour"], as_index=False)
    .agg(trips=("pickup_datetime", "count"))
    .pivot(index="pickup_dow_num", columns="pickup_hour", values="trips")
    .fillna(0)
)
st.plotly_chart(
    px.imshow(heat, aspect="auto", title="Heatmap (dia da semana × hora)"),
    use_container_width=True,
)

# Ranking de zonas (top 15 por trips) — juntando nomes a partir do GeoJSON
by_zone = (
    df_filtered.groupby("pulocationid", as_index=False)
    .agg(trips=("pickup_datetime", "count"),
         revenue_total=("total_amount", "sum") if "total_amount" in df_filtered.columns else ("pickup_datetime","count"))
)
by_zone = by_zone.merge(zone_lkp, on="pulocationid", how="left")
top = (
    by_zone.sort_values("trips", ascending=False)
    .loc[:, ["borough", "zone", "trips", "revenue_total"]]
    .head(15)
)
st.dataframe(top, use_container_width=True)

# Mapa por zona (match por ID: properties.LocationID ↔ pulocationid)
mapdf = by_zone[["pulocationid", "trips"]].rename(columns={"pulocationid": "LocationID"})
fig = px.choropleth_mapbox(
    mapdf,
    geojson=taxi_gj,
    locations="LocationID",
    featureidkey="properties.LocationID",
    color="trips",
    mapbox_style="open-street-map",  # sem token
    zoom=9,
    center={"lat": 40.7128, "lon": -74.0060},
    opacity=0.6,
    title="Pickups por zona (filtrado por data e hora)",
)
st.plotly_chart(fig, use_container_width=True)

st.caption(
    "Todos os visuais e KPIs derivam do mesmo df_filtered (data+hora). "
    "Para renomear zonas/bairros no ranking, os nomes vêm do próprio GeoJSON."
)
