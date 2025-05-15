from kafka import KafkaConsumer, KafkaProducer
import json
import pandas as pd
from datetime import datetime

KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "alert_stream"
OUTPUT_TOPIC = "processed_alerts"

MONITORED_PLACES = ["Tokyo", "Bali", "San Francisco", "California", "Hawaii", "Alaska"]

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="alert-group"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("🚨 Alert Engine nasłuchuje...")

for msg in consumer:
    row = msg.value
    place = str(row.get("place", ""))

    if not any(loc in place for loc in MONITORED_PLACES):
        continue

    try:
        magnitude = float(row["magnitude"])
        risk_level = (
            "Extreme" if magnitude >= 7 else
            "High" if magnitude >= 6 else
            "Moderate" if magnitude >= 5 else
            "Low" if magnitude >= 4 else
            "No risk"
        )

        alert_text = (
            f"ALERT: {risk_level} risk in the region {place}.\n"
            f"Magnitude: {row['magnitude']}, Depth: {row['depth']} km\n"
            f"Risk level: {risk_level}\n"
            f"Recommended actions: Monitor the situation, no immediate actions needed."
        )

        alert = {
            "alert_id": row.get("id"),
            "place": place,
            "time": int(pd.to_datetime(row["time"]).timestamp() * 1000),
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "magnitude": row["magnitude"],
            "depth": row["depth"],
            "alert_text": alert_text,
            "risk_level": risk_level
        }

        producer.send(OUTPUT_TOPIC, alert)
        print(f"✅ Alert wysłany: {place} | M {magnitude}")

    except Exception as e:
        print(f"❌ Błąd w alert_engine: {e}")
