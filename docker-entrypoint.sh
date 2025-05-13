#!/bin/sh

PYTHON="/usr/local/bin/python3"
APP_PATH="/usr/src/app/synchronize.py"
LOG_FILE="/var/log/cron.log"

if [ -n "$CRON" ]; then
    echo "🔁  CRON-Variable gefunden: $CRON"

    # Erzeuge Crontab-Zeile mit den übergebenen Argumenten ($@ wird sofort ausgewertet)
    echo "$CRON $PYTHON $APP_PATH $@ >> $LOG_FILE 2>&1" > /etc/cron.d/syncjob
    chmod 0644 /etc/cron.d/syncjob
    crontab /etc/cron.d/syncjob

    # Logdatei vorbereiten
    touch "$LOG_FILE"

    # Logs im Hintergrund verfolgen
    tail -F "$LOG_FILE" &

    echo "Starte Cron..."
    cron -f
else
    echo "▶️  Keine CRON-Variable gesetzt, führe direkt aus."
    $PYTHON "$APP_PATH" "$@"
fi
