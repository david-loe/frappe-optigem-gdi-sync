# Frappe ↔️ Optigem / GDI Lohn & Gehalt

Syncronisation zwischen Frappe und Optigem / GDI Lohn & Gehalt

## Start

Mit docker

```bash
docker run -v ./config.yaml:/config.yaml davidloe/frappe-optigem-gdi-sync --config /config.yaml
```

oder lokal, [setup](#setup-lokal) und dann:

```bash
python3 synchronize.py
```

```
options:
  -h, --help           show this help message and exit
  --loglevel LOGLEVEL  Setzt das Loglevel (DEBUG, INFO, WARNING, ERROR, CRITICAL)
  --config CONFIG      Pfad zur Konfigurationsdatei
  --dry-run            Führt den Sync im Dry-Run-Modus aus (keine Änderungen werden vorgenommen)
```

## 🕒 Cron-Modus

Wenn die Umgebungsvariable `CRON` gesetzt ist, startet der Container im **Cron-Modus**.
Dabei wird der übergebene Wert von `CRON` als [Cron-Expression](https://crontab.guru/) interpretiert und zur Steuerung des Ausführungszeitpunkts verwendet.

Beispiel:

```bash
docker run -e CRON="*/10 * * * *" -v ./config.yaml:/config.yaml davidloe/frappe-optigem-gdi-sync --config /config.yaml
```

→ Führt das Skript alle 10 Minuten aus.

Wenn `CRON` **nicht gesetzt ist**, wird das Skript **einmalig direkt beim Containerstart** ausgeführt.

## Config anpassen

```bash
cp config.yaml.example config.yaml
```

## Konfiguration der Anwendung

Diese Konfigurationsdatei ermöglicht es, Datenbankverbindungen, den Frappe-Zugang sowie diverse Synchronisationsaufgaben zentral zu definieren. Dadurch können Sie flexibel festlegen, wie Daten zwischen Frappe und unterschiedlichen Datenbanken (MSSQL, Firebird) ausgetauscht werden sollen.

### 1. Databases

Unter `databases` legen Sie eine oder mehrere Datenbankverbindungen fest. Jede Datenbank wird durch einen eindeutigen Schlüssel identifiziert.

- **MSSQL-Datenbank (`type: mssql`):**

  - **server:** Adresse des MSSQL-Servers.
  - **trust_server_certificate:** Boolean, ob dem Serverzertifikat vertraut werden soll.
  - **database:** Name der Datenbank.
  - **user:** Benutzername zur Authentifizierung.
  - **password:** Passwort zur Authentifizierung.

- **Firebird-Datenbank (`type: firebird`):**
  - **host:** Adresse des Firebird-Servers.
  - **port:** Portnummer, unter der der Server erreichbar ist.
  - **charset:** Zeichensatz, Standardwert ist "UTF8".
  - **database:** Name der Datenbank.
  - **user:** Benutzername.
  - **password:** Passwort.

### 2. Frappe

Die `frappe`-Sektion enthält alle notwendigen Informationen, um eine Verbindung zu einer Frappe-Instanz herzustellen:

- **api_key / api_secret:** Zugriffsdaten für die Frappe-API (Pflicht).
- **limit_page_length:** Maximale Anzahl an Einträgen pro Seite (Standard: 20).
- **url:** Basis-URL der Frappe-Instanz (ohne abschließenden Schrägstrich, Pflicht).

### 3. Tasks

Die `tasks`-Sektion definiert die zu synchronisierenden Aufgaben. Jede Aufgabe wird unter einem eigenen Schlüssel definiert und muss den Typ der Synchronisation über das Feld `direction` angeben. Es gibt drei Typen:

- **Bidirektionale Synchronisation (`direction: bidirectional`):**  
  Synchronisiert Daten in beide Richtungen (Frappe ↔ Datenbank).  
  **Wichtige Felder:**

  - **doc_type:** Der in Frappe verwendete Dokumenttyp.
  - **db_name:** Bezeichnung der verwendeten Datenbank (entspricht einem Schlüssel unter `databases`).
  - **mapping:** Dictionary, das Frappe-Felder zu DB-Spalten mappt (alle `key_fields` müssen hier enthalten sein).
  - **key_fields:** Liste der Felder, die als Schlüssel dienen.
  - **table_name:** Name der Zieltabelle in der Datenbank.
  - **frappe:** Enthält Frappe-spezifische Einstellungen, zusätzlich:
    - **fk_id_field:** Fremdschlüssel-Feld zur eindeutigen Identifikation.
    - **modified_fields:** Liste der Änderungs-Timestamps (Default: `["modified"]`); wird auch als `datetime_fields` hinterlegt.
    - **datetime_fields / int_fields:** Felder, die beim Einlesen in Datums- bzw. Ganzzahlen gecastet werden sollen.
  - **db:** Enthält Datenbankspezifische Einstellungen, zusätzlich:
    - **fk_id_field:** Fremdschlüssel-Feld.
    - **id_field:** Identifikationsfeld in der Datenbank.
    - **manual_id_sequence:** Manuelles Hochzählen des Primärschlüssels (Standard: false).
    - **manual_id_sequence_max:** Optionaler Maximalwert für die manuelle Sequenz.
    - **modified_fields:** Liste der Änderungs-Timestamps (Pflicht).
  - **delete:** Gibt an, ob Datensätze gelöscht werden sollen (Standard: true).
  - **datetime_comparison_accuracy_milliseconds:** Genauigkeit beim Vergleich von Datums-/Zeitfeldern in Millisekunden.

- **DB zu Frappe Synchronisation (`direction: db_to_frappe`):**  
  Importiert Daten von der Datenbank nach Frappe.  
  **Wichtige Felder:**

  - **doc_type, db_name, mapping und key_fields:** Wie oben.
  - Es muss **entweder** `table_name` **oder** `query` angegeben werden. Wird `query` genutzt und `use_last_sync_date` ist aktiv, muss zusätzlich `query_with_timestamp` vorhanden sein.
  - **frappe** und **db:** Pflicht, wenn `use_last_sync_date` aktiv ist (Default: true).
  - **process_all:** Boolean, ob alle Datensätze verarbeitet werden sollen (Standard: true).

- **Frappe zu DB Synchronisation (`direction: frappe_to_db`):**  
  Exportiert Daten von Frappe in die Datenbank.  
  **Wichtige Felder:**
  - **doc_type, db_name, mapping und key_fields:** Wie oben.
  - **table_name:** Gibt an, in welche Tabelle die Daten in der Datenbank geschrieben werden sollen (Pflicht).
  - **db:** Enthält u. a. `id_field`, `manual_id_sequence` (Standard: false) und optional `manual_id_sequence_max`.

Zusätzlich gibt es in allen Aufgaben (TaskBase) folgende allgemeine Optionen:

- **create_new:** Legt fest, ob neue Datensätze angelegt werden (Standard: true).
- **use_last_sync_date:** Filtert nur geänderte Datensätze anhand der `modified_fields` (Standard: true). Dann sind `frappe` und `db` Pflicht.
- **value_mapping:** Optionales Mapping pro Frappe-Feld, um Werte zwischen Frappe und DB zu übersetzen.
- **use_strict_value_mapping:** Wenn true, werden unbekannte Werte im Mapping verworfen und es wird ein Warning geloggt.
- **query_with_timestamp:** Muss vorhanden sein, wenn `query` genutzt wird und `use_last_sync_date` aktiv ist.

### 4. Allgemeine Konfiguration

- **dry_run:** Wenn auf `true` gesetzt, werden keine Änderungen an den Systemen vorgenommen – die Ausführung erfolgt als Simulation.
- **timestamp_file:** Pfad zur Datei, in der Zeitstempel der letzten Synchronisation gespeichert werden. (relativ zum Ordner der config Datei)
- **timestamp_buffer_seconds:** Zeitpuffer in Sekunden, um zeitliche Ungenauigkeiten bei der Synchronisation zu kompensieren.

Die Zeitstempel werden in einer SQLite-DB (`timestamps.db` per Default) abgelegt. Für jeden Task-Run wird dort zusätzlich ein Run-Eintrag mit den zugehörigen Log-Meldungen gespeichert.

## Setup Lokal

### 1. pyodbc MSSQL Treiber installieren

https://github.com/mkleehammer/pyodbc/wiki/Install

### 2. Firebird Client Library installieren

https://firebirdsql.org/file/documentation/reference_manuals/driver_manuals/odbc/html/fbodbc205-install.html

### 3. Pakete installieren

```
pip install -r requirements.txt
```
