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
    "trips/",              # alternativas comums
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
        "além de (opcional)
