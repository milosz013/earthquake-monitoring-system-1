import requests
import json
import time
from datetime import datetime
from kafka import KafkaProducer

# Konfiguracja Kafka
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "earthquake_data"

def get_earthquake_data():
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()['features']
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Błąd podczas pobierania danych: {e}")
        return []

def extract_event_info(event):
    props = event['properties']
    geom = event['geometry']
    return {
        'id': event['id'],
        'time': props['time'],
        'place': props['place'],
        'magnitude': props['mag'],
        'longitude': geom['coordinates'][0],
        'latitude': geom['coordinates'][1],
        'depth': geom['coordinates'][2]
    }

def send_to_kafka(producer, topic, data):
    try:
        producer.send(topic, value=data)
        print(f"[{datetime.now()}] ✅ Wysłano do Kafka: {data['id']} – {data['place']}")
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Błąd Kafka: {e}")

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("🚀 Uruchomiono producenta Kafka...")
    seen_ids = set()

    while True:
        print(f"\n🔍 [{datetime.now()}] Sprawdzam nowe dane...")
        events = get_earthquake_data()
        for event in events:
            info = extract_event_info(event)
            if info['id'] not in seen_ids:
                seen_ids.add(info['id'])
                send_to_kafka(producer, KAFKA_TOPIC, info)
        time.sleep(60)

if __name__ == '__main__':
    main()
