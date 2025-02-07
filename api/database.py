from numbers import Number
from typing import Dict
import fdb
import pyodbc
import logging
import sys


class DatabaseConnection:
    def __init__(self, config: Dict):
        self.connections: Dict[str, fdb.Connection | pyodbc.Connection] = {}
        self._setup_connections(config)

    def _setup_connections(self, config):
        databases: Dict[str, Dict[str, str | Number]] = config.get("databases", {})
        if databases:
            for db_name, db_config in databases.items():
                type = db_config.get("type")
                if type == "firebase":
                    self.connections[db_name] = self._connect_firebird(db_name, db_config)
                elif type == "mssql":
                    self.connections[db_name] = self._connect_mssql(db_name, db_config)
                else:
                    raise ValueError(f"Unbekannter Datenbanktyp von {db_name}: '{type}'")

    def _connect_firebird(self, db_name: str, db_config: Dict[str, str | Number]):
        try:
            conn = fdb.connect(
                host=db_config.get("host"),
                port=db_config.get("port"),
                database=db_config.get("database"),
                user=db_config.get("user"),
                password=db_config.get("password"),
                charset="UTF8",
            )
            logging.info(f"Verbindung zur Firebird-Datenbank '{db_name}' hergestellt.")
            return conn
        except Exception as e:
            logging.error(f"Fehler bei der Verbindung zur Firebird-Datenbank '{db_name}': {e}")
            sys.exit(1)

    def _connect_mssql(self, db_name: str, db_config: Dict[str, str | Number]):
        try:
            mssql_conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={db_config.get('server')};"
                f"DATABASE={db_config.get('database')};"
                f"UID={db_config.get('user')};"
                f"PWD={db_config.get('password')};"
                f"TrustServerCertificate={'yes' if db_config.get('trust_server_certificate') else 'no'}"
            )
            logging.info(f"Verbindung zur MSSQL-Datenbank '{db_name}' hergestellt.")
            return pyodbc.connect(mssql_conn_str)
        except Exception as e:
            logging.error(f"Fehler bei der Verbindung zur MSSQL-Datenbank '{db_name}': {e}")
            sys.exit(1)

    def get_connection(self, db_name: str):
        conn = self.connections.get(db_name)
        if conn:
            return conn
        else:
            logging.error(f"Datenbank '{db_name}' nicht gefunden.")
            return None

    def close_connections(self):
        for db_name, conn in self.connections.items():
            conn.close()
            logging.info(f"Verbindung zur Datenbank '{db_name}' geschlossen.")


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
