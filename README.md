# Frappe ↔️ Optigem / GDI Lohn & Gehalt

Syncronisation zwischen Frappe und Optigem / GDI Lohn & Gehalt

## Run

using docker

```
docker run -v ./config.yaml:/config.yaml davidloe/frappe-optigem-gdi-sync --config /config.yaml
```

or running locally

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

**Erforderliche Felder je nach Synchronisationsrichtung:**

- `db_to_frappe`:
  - `endpoint`
  - `query`
  - `mapping`
  - `db_name`
  - Optional: `name` (für logs)
  - Optional: `key_fields` (für Updates)
  - Optional: `create_new` (wenn `true` werden neue Dokumente bei Frappe erstellt, für die kein match anhand der `key_fields` gefunden wurde)
  - Optional: `process_all` (`true` wenn alle gefundenen Dokumente verarbeitet werden sollen, andernfalls nur das erste)
- `frappe_to_db`:
  - `endpoint`
  - `table_name`
  - `mapping`
  - `db_name`
  - Optional: `name` (für logs)
  - Optional: `key_fields` (für Updates)
  - Optional: `create_new` (wenn `true` werden neue Dokumente in der Datenbank eingefügt, für die kein match anhand der `key_fields` gefunden wurde)

## Setup Local

### 1. pyodbc MSSQL Treiber installieren

https://github.com/mkleehammer/pyodbc/wiki/Install

### 2. Firebird Client Library installieren

https://firebirdsql.org/file/documentation/reference_manuals/driver_manuals/odbc/html/fbodbc205-install.html

### 3. Pakete installieren

```
pip install -r requirements.txt
```
