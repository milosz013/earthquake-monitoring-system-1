from kafka import KafkaConsumer
import pandas as pd
from datetime import datetime, timedelta, timezone
import matplotlib.pyplot as plt
import os
import json

# Kafka i pliki wyjściowe
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "processed_alerts"
CSV_OUT = "raport_alerty_dzienny.csv"
XLSX_OUT = "raport_alerty_dzienny.xlsx"
PLOT_OUT = "raport_wykres.png"

# Parametry raportu
WINDOW_HOURS = 24
MAX_RECORDS = 500  # zabezpieczenie przed nieskończonym nasłuchiwaniem

try:
    print("📊 Generowanie raportu dziennego z Kafka...")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="report-generator"
    )

    # Zbieranie wiadomości
    records = []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=WINDOW_HOURS)

    print(f"⏳ Oczekiwanie na dane z Kafka (max {MAX_RECORDS} rekordów)...")

    for msg in consumer:
        event = msg.value

        try:
            event["time"] = pd.to_datetime(event["time"], unit="ms", utc=True)
        except:
            continue

        if event["time"] >= cutoff:
            records.append(event)

        if len(records) >= MAX_RECORDS:
            break

    consumer.close()

    if not records:
        print("⚠️ Brak danych do raportu.")
        exit()

    df = pd.DataFrame(records)

    print(f"✅ Zebrano {len(df)} rekordów z ostatnich 24h.")

    # Zapis CSV
    df.to_csv(CSV_OUT, index=False)
    print(f"💾 Zapisano: {CSV_OUT}")

    # Zapis Excel (bez strefy czasowej)
    df["time"] = df["time"].dt.tz_localize(None)
    df.to_excel(XLSX_OUT, index=False)
    print(f"💾 Zapisano: {XLSX_OUT}")

    # Wykres alertów wg poziomu ryzyka
    alert_counts = df["risk_level"].value_counts().sort_index()
    alert_counts.plot(kind="bar", title="Alerty wg poziomu ryzyka (24h)", color="darkorange")
    plt.xlabel("Poziom ryzyka")
    plt.ylabel("Liczba alertów")
    plt.tight_layout()
    plt.savefig(PLOT_OUT)
    print(f"📈 Wykres zapisany: {PLOT_OUT}")

except Exception as e:
    print(f"❌ Błąd podczas generowania raportu: {e}")
