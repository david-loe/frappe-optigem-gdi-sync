from datetime import datetime, timedelta, timezone
import fdb
import pyodbc
import logging
import sys

from config import DatabaseConfig, FirebirdDatabaseConfig, MssqlDatabaseConfig


class DatabaseConnection:
    def __init__(self, database_configs: dict[str, DatabaseConfig]):
        self.connections: dict[str, fdb.Connection | pyodbc.Connection] = {}
        self._setup_connections(database_configs)

    def _setup_connections(self, database_configs: dict[str, DatabaseConfig]):
        if database_configs:
            for db_name, db_config in database_configs.items():
                if db_config.type == "firebird":
                    self.connections[db_name] = self._connect_firebird(db_name, db_config)
                elif db_config.type == "mssql":
                    self.connections[db_name] = self._connect_mssql(db_name, db_config)

    def _connect_firebird(self, db_name: str, db_config: FirebirdDatabaseConfig):
        try:
            conn = fdb.connect(
                host=db_config.host,
                port=db_config.port,
                database=db_config.database,
                user=db_config.user,
                password=db_config.password,
                charset=db_config.charset,
            )
            logging.info(f"Verbindung zur Firebird-Datenbank '{db_name}' hergestellt.")
            get_time_zone(conn)
            return conn
        except Exception as e:
            logging.error(f"Fehler bei der Verbindung zur Firebird-Datenbank '{db_name}': {e}")
            sys.exit(1)

    def _connect_mssql(self, db_name: str, db_config: MssqlDatabaseConfig):
        try:
            mssql_conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={db_config.server};"
                f"DATABASE={db_config.database};"
                f"UID={db_config.user};"
                f"PWD={db_config.password};"
                f"TrustServerCertificate={'yes' if db_config.trust_server_certificate else 'no'}"
            )
            conn = pyodbc.connect(mssql_conn_str)
            logging.info(f"Verbindung zur MSSQL-Datenbank '{db_name}' hergestellt.")
            get_time_zone(conn)
            return conn
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


def get_time_zone(db_conn: fdb.Connection | pyodbc.Connection):
    minutes: int = None
    cursor = db_conn.cursor()
    if isinstance(db_conn, fdb.Connection):
        sql = "SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE;"
        try:
            cursor.execute(sql)
            current_db_time: datetime = cursor.fetchone()[0]
            minutes = round((current_db_time - datetime.now(timezone.utc).replace(tzinfo=None)).total_seconds() / 60)
        except Exception as e:
            logging.error(f"Fehler beim Ausführen der Query '{sql}'")
            logging.error(e)
        finally:
            cursor.close()
    else:
        sql = "SELECT DATEPART(TZOFFSET, SYSDATETIMEOFFSET()) AS TimeZoneOffsetMinutes;"
        try:
            cursor.execute(sql)
            minutes = cursor.fetchone()[0]
        except Exception as e:
            logging.error(f"Fehler beim Ausführen der Query '{sql}'")
            logging.error(e)
        finally:
            cursor.close()
    if minutes is not None:
        return timedelta(minutes=minutes)


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
