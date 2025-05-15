@echo off
title TravelQuake - uruchamianie systemu
cd /d %~dp0

echo 🔄 Uruchamianie Dockera...
docker compose up -d

echo ⏳ Oczekiwanie 10 sekund na uruchomienie Kafka...
timeout /t 10 /nobreak > nul

echo 🔥 Start systemu TravelQuake...
python main.py

pause

