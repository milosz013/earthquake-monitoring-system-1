import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh
from kafka import KafkaConsumer
import json

KAFKA_BROKER = "localhost:9092"
TOPIC = "processed_alerts"
REFRESH_MS = 5000

st.set_page_config(page_title="🌍 TravelQuake Map", layout="wide")
st.title("🗺️ TravelQuake – Alert Map (last 24h)")
st_autorefresh(interval=REFRESH_MS, key="autorefresh")

try:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='map-group'
    )

    records = []
    for message in consumer:
        event = message.value
        if not all(k in event for k in ["latitude", "longitude", "time", "alert_text", "risk_level"]):
            continue
        try:
            event["latitude"] = float(event["latitude"])
            event["longitude"] = float(event["longitude"])
            event["time"] = pd.to_datetime(event["time"], unit="ms", utc=True)
            records.append(event)
        except:
            continue

        if len(records) >= 100:
            break

    df = pd.DataFrame(records)
    df = df.dropna(subset=["latitude", "longitude"])

    # Filtruj ostatnie 24h
    now = datetime.now(timezone.utc)
    df = df[df["time"] >= now - timedelta(hours=24)]

    if df.empty:
        st.warning("🔕 Brak alertów z ostatnich 24h.")
        start_coords = [0, 0]
    else:
        start_coords = [df.iloc[0]["latitude"], df.iloc[0]["longitude"]]

    m = folium.Map(location=start_coords, zoom_start=4)

    for _, row in df.iterrows():
        popup = f"""
        <div style='font-family:sans-serif; font-size:14px'>
            <b>📍 {row['place']}</b><br>
            <b>Magnitude:</b> {row['magnitude']}<br>
            <b>Depth:</b> {row['depth']} km<br>
            <b>Risk:</b> {row['risk_level']}<br><hr>
            <small>{row['alert_text'].replace(chr(10), '<br>')}</small>
        </div>
        """
        color = "red" if row["risk_level"].lower() not in ["no risk", "low"] else "green"
        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            popup=popup,
            icon=folium.Icon(color=color)
        ).add_to(m)

    st.markdown("<div style='display: flex; justify-content: center;'>", unsafe_allow_html=True)
    st_folium(m, width=1000, height=600)
    st.markdown("</div>", unsafe_allow_html=True)

except Exception as e:
    st.error(f"❌ Błąd połączenia z Kafka: {e}")
