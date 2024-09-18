import fdb
import pyodbc
import logging
import sys

class DatabaseConnection:
    def __init__(self, config):
        self.firebird_conn = None
        self.mssql_conn = None
        self._setup_connections(config)

    def _setup_connections(self, config):
        self._connect_firebird(config.get('firebird'))
        self._connect_mssql(config.get('mssql'))

    def _connect_firebird(self, config):
        if config:
            try:
                self.firebird_conn = fdb.connect(
                    host=config['host'],
                    database=config['database'],
                    user=config['user'],
                    password=config['password'],
                    charset='UTF8'
                )
                logging.info("Verbindung zur Firebird-Datenbank hergestellt.")
            except Exception as e:
                logging.error(f"Fehler bei der Verbindung zur Firebird-Datenbank: {e}")
                sys.exit(1)

    def _connect_mssql(self, config):
        if config:
            try:
                mssql_conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={config['server']};"
                    f"DATABASE={config['database']};"
                    f"UID={config['user']};"
                    f"PWD={config['password']}"
                )
                self.mssql_conn = pyodbc.connect(mssql_conn_str)
                logging.info("Verbindung zur MSSQL-Datenbank hergestellt.")
            except Exception as e:
                logging.error(f"Fehler bei der Verbindung zur MSSQL-Datenbank: {e}")
                sys.exit(1)

    def get_connection(self, db_type):
        if db_type == 'firebird':
            return self.firebird_conn
        elif db_type == 'mssql':
            return self.mssql_conn
        else:
            logging.error(f"Unbekannter Datenbanktyp: {db_type}")
            return None

    def close_connections(self):
        if self.firebird_conn:
            self.firebird_conn.close()
        if self.mssql_conn:
            self.mssql_conn.close()