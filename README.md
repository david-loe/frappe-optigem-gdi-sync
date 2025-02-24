# Frappe ↔️ Optigem / GDI Lohn & Gehalt

Syncronisation zwischen Frappe und Optigem / GDI Lohn & Gehalt

## Start

Mit docker

```
docker run -v ./config.yaml:/config.yaml davidloe/frappe-optigem-gdi-sync --config /config.yaml
```

oder lokal, [setup](#setup-local) und dann:

```
synchronize.py
```

```
options:
  -h, --help           show this help message and exit
  --loglevel LOGLEVEL  Setzt das Loglevel (DEBUG, INFO, WARNING, ERROR, CRITICAL)
  --config CONFIG      Pfad zur Konfigurationsdatei
  --dry-run            Führt den Sync im Dry-Run-Modus aus (keine Änderungen werden vorgenommen)
```

## Config anpassen

```
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

- **api_key:** API-Schlüssel für den Zugriff.
- **api_secret:** API-Geheimnis.
- **limit_page_length:** Maximale Anzahl an Einträgen pro Seite (Standard ist 20, im Beispiel z. B. auf 50 gesetzt).
- **url:** Basis-URL der Frappe-Instanz (ohne abschließenden Schrägstrich).

### 3. Tasks

Die `tasks`-Sektion definiert die zu synchronisierenden Aufgaben. Jede Aufgabe wird unter einem eigenen Schlüssel definiert und muss den Typ der Synchronisation über das Feld `direction` angeben. Es gibt drei Typen:

- **Bidirektionale Synchronisation (`direction: bidirectional`):**  
  Synchronisiert Daten in beide Richtungen (Frappe ↔ Datenbank).  
  **Wichtige Felder:**

  - **doc_type:** Der in Frappe verwendete Dokumenttyp.
  - **db_name:** Bezeichnung der verwendeten Datenbank (entspricht einem Schlüssel unter `databases`).
  - **mapping:** Ein Dictionary, das die Zuordnung von Frappe-Feldern zu Datenbankfeldern definiert.
  - **key_fields:** Liste der Felder, die als Schlüssel dienen und im Mapping vorhanden sein müssen.
  - **table_name:** Name der Zieltabelle in der Datenbank.
  - **frappe:** Enthält Frappe-spezifische Einstellungen, zusätzlich:
    - **fk_id_field:** Fremdschlüssel-Feld zur eindeutigen Identifikation.
  - **db:** Enthält Datenbankspezifische Einstellungen, zusätzlich:
    - **fk_id_field:** Fremdschlüssel-Feld.
    - **id_field:** Identifikationsfeld in der Datenbank.
  - **delete:** Gibt an, ob Datensätze gelöscht werden sollen (Standard: true).
  - **datetime_comparison_accuracy_milliseconds:** Genauigkeit beim Vergleich von Datums-/Zeitfeldern in Millisekunden.

- **DB zu Frappe Synchronisation (`direction: db_to_frappe`):**  
  Importiert Daten von der Datenbank nach Frappe.  
  **Wichtige Felder:**

  - **doc_type, db_name, mapping und key_fields:** Wie oben.
  - Es muss **entweder** `table_name` **oder** `query` angegeben werden. Im Beispiel wird `table_name` genutzt.
  - **frappe** und **db:** Konfigurationen zur Nutzung des letzten Synchronisationsdatums (wichtig, wenn `use_last_sync_date` aktiviert ist).
  - **process_all:** Boolean, ob alle Datensätze verarbeitet werden sollen (Standard: true).

- **Frappe zu DB Synchronisation (`direction: frappe_to_db`):**  
  Exportiert Daten von Frappe in die Datenbank.  
  **Wichtige Felder:**
  - **doc_type, db_name, mapping und key_fields:** Wie oben.
  - **table_name:** Gibt an, in welche Tabelle die Daten in der Datenbank geschrieben werden sollen.

Zusätzlich gibt es in allen Aufgaben (TaskBase) folgende allgemeine Optionen:

- **create_new:** Legt fest, ob neue Datensätze angelegt werden sollen (Standard: true).
- **use_last_sync_date:** Gibt an, ob das letzte Synchronisationsdatum zur Filterung von Änderungen verwendet wird (Standard: true).
- **query_with_timestamp:** Muss angegeben werden, wenn eine eigene Query verwendet wird und `use_last_sync_date` aktiviert ist.

### 4. Allgemeine Konfiguration

- **dry_run:** Wenn auf `true` gesetzt, werden keine Änderungen an den Systemen vorgenommen – die Ausführung erfolgt als Simulation.
- **timestamp_file:** Pfad zur Datei, in der Zeitstempel der letzten Synchronisation gespeichert werden.
- **timestamp_buffer_seconds:** Zeitpuffer in Sekunden, um zeitliche Ungenauigkeiten bei der Synchronisation zu kompensieren.

## Setup Lokal

### 1. pyodbc MSSQL Treiber installieren

https://github.com/mkleehammer/pyodbc/wiki/Install

### 2. Firebird Client Library installieren

https://firebirdsql.org/file/documentation/reference_manuals/driver_manuals/odbc/html/fbodbc205-install.html

### 3. Pakete installieren

```
pip install -r requirements.txt
```
