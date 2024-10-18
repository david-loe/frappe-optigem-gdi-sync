# Frappe ↔️ Optigem / GDI Lohn & Gehalt

Syncronisation zwischen Frappe und Optigem / GDI Lohn & Gehalt

## Setup

### 1. Pakete installieren

```
pip install -r requirements.txt
```

### 2. pyodbc MSSQL Treiber installieren

https://github.com/mkleehammer/pyodbc/wiki/Install

### 3. Firebird Client Library installieren

https://firebirdsql.org/file/documentation/reference_manuals/driver_manuals/odbc/html/fbodbc205-install.html

### 4. Config anpassen

```
cp config.yaml.example config.yaml
```

**Erforderliche Felder je nach Synchronisationsrichtung:**

- `db_to_frappe`:
  - `endpoint`
  - `query`
  - `mapping`
  - `db_type`
  - `db_name`
  - Optional: `key_fields` (für Updates)
  - Optional: `create_new` (wenn `true` werden neue Dokumente bei Frappe erstellt, für die kein match anhand der `key_fields` gefunden wurde)
  - Optional: `process_all` (`true` wenn alle gefundenen Dokumente verarbeitet werden sollen, andernfalls nur das erste)
- `frappe_to_db`:
  - `endpoint`
  - `table_name`
  - `mapping`
  - `db_type`
  - `db_name`
  - Optional: `key_fields` (für Updates)
  - Optional: `create_new` (wenn `true` werden neue Dokumente in der Datenbank eingefügt, für die kein match anhand der `key_fields` gefunden wurde)

## Run

```
python sync.py
```

```
options:
  -h, --help           show this help message and exit
  --loglevel LOGLEVEL  Setzt das Loglevel (DEBUG, INFO, WARNING, ERROR, CRITICAL)
  --config CONFIG      Pfad zur Konfigurationsdatei
  --dry-run            Führt den Sync im Dry-Run-Modus aus (keine Änderungen werden vorgenommen)
```
