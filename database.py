from typing import Dict
import fdb
import pyodbc
import logging
import sys


class DatabaseConnection:
    def __init__(self, config):
        self.firebird_conns: Dict[str, fdb.Connection] = {}
        self.mssql_conns: Dict[str, pyodbc.Connection] = {}
        self._setup_connections(config)

    def _setup_connections(self, config):
        self._connect_firebird(config.get("firebird", {}))
        self._connect_mssql(config.get("mssql", {}))

    def _connect_firebird(self, config):
        for db_name, db_config in config.items():
            try:
                conn = fdb.connect(
                    host=db_config["host"],
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

    def _connect_mssql(self, config):
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
