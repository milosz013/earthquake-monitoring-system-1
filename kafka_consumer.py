from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime, timedelta
from datetime import timezone

# Kafka
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "earthquake_data"
OUTPUT_TOPIC = "alert_stream"

# Funkcja filtrująca
def should_alert(event):
    try:
        mag = float(event['magnitude'])
        depth = float(event['depth'])
        event_time = datetime.fromtimestamp(event['time'] / 1000.0, tz=timezone.utc)
        now = datetime.now(timezone.utc)
        return mag > 4.5 and depth < 70 and (now - event_time) < timedelta(minutes=60)
    except:
        return False

# Konsument i producent
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="quake-consumer"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("✅ Kafka consumer działa – filtruje i przesyła alerty do alert_stream")

for msg in consumer:
    event = msg.value
    try:
        # if should_alert(event):
        #     print(f"📤 Alert przesłany: {event['place']} | M {event['magnitude']}")
        #     producer.send(OUTPUT_TOPIC, event)
        # else:
        #     print(f"⏭️ Pominięto (niskie ryzyko): {event.get('place', 'brak')}")
        print(f"📤 Alert przesłany: {event['place']} | M {event['magnitude']}")
        producer.send(OUTPUT_TOPIC, event)


    except Exception as e:
        print(f"❌ Błąd: {e}")
