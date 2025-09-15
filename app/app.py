import os, json, pandas as pd, plotly.express as px, streamlit as st

st.set_page_config(page_title="NYC Yellow Taxi — Jun–Jul 2025", layout="wide")

# ===== CONFIG QUE VOCÊ PODE MUDAR =====
BUCKET = os.getenv("BUCKET", "nyc-taxi-portfolio-frm")  # troque se usar outro bucket
PREFIX = os.getenv("PREFIX", "agg")                      # onde estão os CTAS
S3_ANON = os.getenv("S3_ANON", "true").lower() == "true" # True se agg/* for público

S3_PATH = f"s3://{BUCKET}/{PREFIX}"
STORAGE_OPTS = {"anon": True} if S3_ANON else {}

# ===== CARREGA PARQUETS (gerados no Athena CTAS) =====
daily   = pd.read_parquet(f"{S3_PATH}/agg_daily/", storage_options=STORAGE_OPTS)
hourdow = pd.read_parquet(f"{S3_PATH}/agg_hour_dow/", storage_options=STORAGE_OPTS)
zonepu  = pd.read_parquet(f"{S3_PATH}/agg_zone_pickup/", storage_options=STORAGE_OPTS)
pay     = pd.read_parquet(f"{S3_PATH}/agg_payment/", storage_options=STORAGE_OPTS)

# ===== GEOJSON LOCAL (você vai subir em data/taxi_zones.geojson) =====
with open("data/taxi_zones.geojson", "r", encoding="utf-8") as f:
    taxi_gj = json.load(f)

st.title("🚕 NYC Yellow Taxi — Jun–Jul 2025")
st.caption("Fonte: NYC TLC (Parquet), pré-agg via Athena CTAS. Dashboard em Streamlit.")

# ===== FILTROS =====
min_d, max_d = daily["pickup_date"].min(), daily["pickup_date"].max()
c1, c2 = st.columns([2,1])
dr = c1.date_input("Período", [min_d, max_d], min_value=min_d, max_value=max_d)
hr_min, hr_max = c2.select_slider("Hora (pickup)", options=list(range(24)), value=(0,23))

d0, d1 = pd.to_datetime(dr[0]), pd.to_datetime(dr[1])
daily_f   = daily[(daily.pickup_date>=d0)&(daily.pickup_date<=d1)]
hourdow_f = hourdow[(hourdow.pickup_hour>=hr_min)&(hourdow.pickup_hour<=hr_max)]
zonepu_f  = zonepu
pay_f     = pay[(pay.pickup_date>=d0)&(pay.pickup_date<=d1)]

# ===== KPIs =====
k1,k2,k3,k4,k5 = st.columns(5)
k1.metric("Viagens", f"{int(daily_f['trips'].sum()):,}")
k2.metric("Receita ($)", f"{daily_f['revenue_total'].sum():,.0f}")
k3.metric("Tarifa média ($)", f"{daily_f['avg_fare'].mean():.2f}")
k4.metric("Tip % médio", f"{100*daily_f['avg_tip_pct'].mean():.1f}%")
k5.metric("Distância média (mi)", f"{daily_f['avg_trip_miles'].mean():.2f}")

# ===== SÉRIE DIÁRIA =====
st.plotly_chart(px.line(daily_f, x="pickup_date", y="trips", title="Viagens por dia"),
                use_container_width=True)

# ===== HEATMAP HORA × DIA-SEMANA =====
heat = hourdow_f.pivot_table(index="pickup_dow_num", columns="pickup_hour",
                             values="trips", aggfunc="sum").fillna(0)
st.plotly_chart(px.imshow(heat, aspect="auto", title="Heatmap (dia da semana × hora)"),
                use_container_width=True)

# ===== RANKING ZONAS =====
top = (zonepu_f.groupby(["borough","zone"], as_index=False)
       .agg(trips=("trips","sum"), revenue=("revenue_total","sum"))
       .sort_values("trips", ascending=False).head(15))
st.dataframe(top, use_container_width=True)

# ===== MAPA (Pickup por zona) =====
zone_counts = zonepu_f.groupby("zone", as_index=False)["trips"].sum()
fig = px.choropleth_mapbox(
    zone_counts, geojson=taxi_gj, locations="zone", featureidkey="properties.zone",
    color="trips", mapbox_style="carto-positron", zoom=9,
    center={"lat":40.7128,"lon":-74.0060}, opacity=0.6, title="Pickups por zona"
)
st.plotly_chart(fig, use_container_width=True)

st.caption("CTAS do Athena grava Parquet em S3; app lê direto do prefixo agg/ (público ou via Secrets).")
