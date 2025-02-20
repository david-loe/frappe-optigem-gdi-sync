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

Diese Anwendung nutzt eine YAML-Konfiguration, um Datenbankverbindungen, Frappe-API-Zugriffe und synchronisationsbezogene Tasks zu definieren. Im Folgenden werden alle Konfigurationsmöglichkeiten erläutert.

### 1. Global

- **dry_run**:  
  _Typ_: Boolean (default: `false`)  
  _Beschreibung_: Wenn auf `true` gesetzt, wird kein schreibender Zugriff auf Datenbanken oder Frappe vorgenommen – es erfolgt ein sogenannter Trockenlauf.

### 2. Datenbanken (databases)

Hier können mehrere Datenbanken definiert werden. Jeder Eintrag benötigt einen eindeutigen Schlüssel (Name der DB-Konfiguration).

#### Unterstützte Typen:

- **MSSQL**  
  _Kennzeichen_: `type: mssql`  
  _Erforderliche Felder_:

  - `database`: Name der Datenbank
  - `user`: Benutzername
  - `password`: Passwort
  - `server`: Serveradresse (z.B. IP oder Hostname)
  - `trust_server_certificate`: (Boolean) Gibt an, ob das Serverzertifikat vertraut werden soll (Standard: `false`).

- **Firebird**  
  _Kennzeichen_: `type: firebird`  
  _Erforderliche Felder_:
  - `database`: Name der Datenbank
  - `user`: Benutzername
  - `password`: Passwort
  - `host`: Hostadresse
  - `port`: Portnummer
  - `charset`: Zeichensatz (Standard: `UTF8`).

### 3. Frappe-Konfiguration (frappe)

Diese Einstellungen definieren den Zugang zur Frappe API.

- **api_key**:  
  _Typ_: String  
  _Beschreibung_: API-Schlüssel für den Zugriff auf Frappe.

- **api_secret**:  
  _Typ_: String  
  _Beschreibung_: API-Geheimnis für den Zugriff auf Frappe.

- **limit_page_length**:  
  _Typ_: Integer  
  _Beschreibung_: Legt fest, wie viele Datensätze maximal pro Seite abgefragt werden (Standard: `20`).

### 4. Tasks (tasks)

Hier werden die Synchronisationsaufgaben definiert. Jede Task konfiguriert den Datenaustausch zwischen einer Datenbank und Frappe. Es gibt drei verschiedene Task-Typen, die über das Feld `direction` unterschieden werden:

#### Gemeinsame Felder (für alle Tasks):

- **direction**:  
  _Typ_: Literal (mögliche Werte: `bidirectional`, `db_to_frappe`, `frappe_to_db`)  
  _Beschreibung_: Legt die Richtung der Synchronisation fest.

- **name**:  
  _Typ_: String  
  _Beschreibung_: Bezeichner der Task. Manche Typen haben einen Standardwert.

- **doc_type**:  
  _Typ_: String  
  _Beschreibung_: API-Endpunkt in Frappe, der angesprochen wird.

- **db_name**:  
  _Typ_: String  
  _Beschreibung_: Verweist auf den Schlüssel der in `databases` definierten Datenbank, die verwendet wird.

- **mapping**:  
  _Typ_: Dictionary (`str` -> `str`)  
  _Beschreibung_: Ordnet Felder zwischen Frappe und der Datenbank zu.  
  **Wichtig:** Alle in `key_fields` aufgeführten Felder müssen als Schlüssel in diesem Mapping vorhanden sein.

- **key_fields**:  
  _Typ_: Liste von Strings  
  _Beschreibung_: Felder, die als Schlüssel zur Identifikation von Datensätzen dienen und zwingend im Mapping vorhanden sein müssen.

- **table_name**:  
  _Typ_: String (optional, je nach Task-Typ)  
  _Beschreibung_: Name der Tabelle in der Datenbank, aus der Daten gelesen oder in die Daten geschrieben werden.

- **query**:  
  _Typ_: String (optional)  
  _Beschreibung_: Alternative zur `table_name`-Angabe. Hier kann eine SQL-Abfrage angegeben werden.  
  **Hinweis:** Bei Tasks des Typs `db_to_frappe` muss mindestens entweder `table_name` oder `query` angegeben werden.

- **process_all**:  
  _Typ_: Boolean (default: `false`)  
  _Beschreibung_: Gibt an, ob alle Datensätze verarbeitet werden sollen.

- **create_new**:  
  _Typ_: Boolean (default: `false`)  
  _Beschreibung_: Gibt an, ob neue Einträge in Frappe erstellt werden sollen, falls diese noch nicht vorhanden sind.

#### Spezifische Task-Typen:

#### 4.1 Bidirectional Task (`direction: bidirectional`)

- **Zusätzliche Felder**:
  - **frappe**:  
    Enthält die Frappe-spezifische Task-Konfiguration:
    - `modified_field`: Feld in Frappe, das das Änderungsdatum enthält.
    - `fk_id_field`: Fremdschlüssel-Feld in Frappe.
  - **db**:  
    Enthält die Datenbank-spezifische Task-Konfiguration:
    - `modified_field`: Feld in der Datenbank, das das Änderungsdatum enthält.
    - `fk_id_field`: Fremdschlüssel-Feld in der Datenbank.
    - `fallback_modified_field`: (Optional) Ein alternatives Feld für das Änderungsdatum.

#### 4.2 DB -> Frappe Task (`direction: db_to_frappe`)

- **Wichtig**:  
  Entweder `table_name` oder `query` muss angegeben werden, damit die Datenquelle eindeutig bestimmt ist.

#### 4.3 Frappe -> DB Task (`direction: frappe_to_db`)

- **Erforderlich**:  
  Das Feld `table_name` muss angegeben werden, da die Daten in die Datenbank geschrieben werden sollen.

## Setup Lokal

### 1. pyodbc MSSQL Treiber installieren

https://github.com/mkleehammer/pyodbc/wiki/Install

### 2. Firebird Client Library installieren

https://firebirdsql.org/file/documentation/reference_manuals/driver_manuals/odbc/html/fbodbc205-install.html

### 3. Pakete installieren

```
pip install -r requirements.txt
```
