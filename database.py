from numbers import Number
from typing import Dict
import fdb
import pyodbc
import logging
import sys


class DatabaseConnection:
    def __init__(self, config: Dict):
        self.firebird_conns: Dict[str, fdb.Connection] = {}
        self.mssql_conns: Dict[str, pyodbc.Connection] = {}
        self._setup_connections(config)

    def _setup_connections(self, config):
        self._connect_firebird(config.get("firebird", {}))
        self._connect_mssql(config.get("mssql", {}))

    def _connect_firebird(self, config: Dict[str, Dict[str, str | Number]]):
        if config:
            for db_name, db_config in config.items():
                try:
                    conn = fdb.connect(
                        host=db_config["host"],
                        port=db_config["port"],
                        database=db_config["database"],
                        user=db_config["user"],
                        password=db_config["password"],
                        charset="UTF8",
                    )
                    self.firebird_conns[db_name] = conn
                    logging.info(f"Verbindung zur Firebird-Datenbank '{db_name}' hergestellt.")
                except Exception as e:
                    logging.error(f"Fehler bei der Verbindung zur Firebird-Datenbank '{db_name}': {e}")
                    sys.exit(1)

    def _connect_mssql(self, config: Dict[str, Dict[str, str | Number]]):
        if config:
            for db_name, db_config in config.items():
                try:
                    mssql_conn_str = (
                        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                        f"SERVER={db_config['server']};"
                        f"DATABASE={db_config['database']};"
                        f"UID={db_config['user']};"
                        f"PWD={db_config['password']}"
                    )
                    conn = pyodbc.connect(mssql_conn_str)
                    self.mssql_conns[db_name] = conn
                    logging.info(f"Verbindung zur MSSQL-Datenbank '{db_name}' hergestellt.")
                except Exception as e:
                    logging.error(f"Fehler bei der Verbindung zur MSSQL-Datenbank '{db_name}': {e}")
                    sys.exit(1)

    def get_connection(self, db_type, db_name):
        if db_type == "firebird":
            conn = self.firebird_conns.get(db_name)
            if not conn:
                logging.error(f"Firebird-Datenbank '{db_name}' nicht gefunden.")
            return conn
        elif db_type == "mssql":
            conn = self.mssql_conns.get(db_name)
            if not conn:
                logging.error(f"MSSQL-Datenbank '{db_name}' nicht gefunden.")
            return conn
        else:
            logging.error(f"Unbekannter Datenbanktyp: {db_type}")
            return None

    def close_connections(self):
        for db_name, conn in self.firebird_conns.items():
            conn.close()
            logging.info(f"Verbindung zur Firebird-Datenbank '{db_name}' geschlossen.")
        for db_name, conn in self.mssql_conns.items():
            conn.close()
            logging.info(f"Verbindung zur MSSQL-Datenbank '{db_name}' geschlossen.")


def format_query(query, params):
    # Diese Funktion ersetzt die Platzhalter in der Abfrage durch die Parameterwerte
    # für Logging-Zwecke. Sie stellt sicher, dass Strings korrekt gequotet werden.
    from datetime import datetime, date

    def format_param(param):
        if isinstance(param, str):
            escaped = param.replace("'", "''")
            return f"'{escaped}'"  # Einzelne Anführungszeichen escapen
        elif isinstance(param, (int, float)):
            return str(param)
        elif isinstance(param, (datetime, date)):
            return f"'{param.isoformat()}'"
        elif param is None:
            return "NULL"
        else:
            return str(param)

    formatted_params = [format_param(p) for p in params]
    # Aufteilen der Abfrage anhand der Platzhalter
    query_parts = query.split("?")
    full_query = ""
    for i in range(len(query_parts)):
        full_query += query_parts[i]
        if i < len(formatted_params):
            full_query += formatted_params[i]
    return full_query
