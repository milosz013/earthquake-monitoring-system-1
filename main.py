import subprocess
import time
import socket
import os
import sys
import importlib.util

def run_background(script):
    print(f"\n🚀 Uruchamianie: {script}")
    return subprocess.Popen(["python", script])

def install_streamlit():
    print("📦 Instaluję brakujące pakiety: streamlit, streamlit-folium, folium...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "streamlit", "streamlit-folium", "folium"])

def is_package_installed(package):
    return importlib.util.find_spec(package) is not None

def run_streamlit(path):
    print("🌍 Uruchamianie mapy Streamlit...")
    if not os.path.exists(path):
        print(f"❌ Nie znaleziono pliku: {path}")
        return None
    return subprocess.Popen(["python", "-m", "streamlit", "run", path])

def wait_for_kafka(host="localhost", port=9092, timeout=30):
    print("⏳ Oczekiwanie na uruchomienie Kafka (localhost:9092)...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                print("✅ Kafka dostępna!")
                return True
        except OSError:
            time.sleep(1)
    print("❌ Kafka nie odpowiada – upewnij się, że kontener działa.")
    return False

def main():
    print("🔥 Start systemu TravelQuake...")

    if not wait_for_kafka():
        sys.exit(1)

    # Streamlit – sprawdź czy zainstalowany
    if not is_package_installed("streamlit"):
        install_streamlit()

    processes = []

    # Uruchamianie komponentów
    processes.append(run_background("earthquake_producer.py"))
    time.sleep(2)

    processes.append(run_background("kafka_consumer.py"))
    time.sleep(1)

    processes.append(run_background("alert_engine.py"))
    time.sleep(1)

    processes.append(run_streamlit("visualizor.py"))

    print("\n✅ System działa w pełni na bazie Kafka.")
    print("❗ Aby zakończyć – naciśnij Ctrl + C")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Zatrzymywanie procesów...")
        for p in processes:
            if p:
                p.terminate()

if __name__ == "__main__":
    main()
