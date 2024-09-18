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

## Run

```
python sync.py
```
