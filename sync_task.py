import logging

from database import DatabaseConnection
from frappe import FrappeAPI


class SyncTask:
    def __init__(self, task_config, db_conn: DatabaseConnection, frappe_api: FrappeAPI):
        self.endpoint = task_config.get("endpoint")
        self.mapping = task_config.get("mapping")
        self.db_type = task_config.get("db_type")
        self.db_name = task_config.get("db_name")
        self.direction = task_config.get("direction", "db_to_frappe")
        self.key_fields = task_config.get("key_fields")
        self.process_all = task_config.get("process_all", False)
        self.frappe_api = frappe_api

        self._check_config(task_config)
        self.db_conn = db_conn.get_connection(self.db_type, self.db_name)

    def _check_config(self, task_config):
        # Konfigurationsprüfung basierend auf der Synchronisationsrichtung
        if self.direction == "db_to_frappe":
            required_fields = ["endpoint", "query", "mapping", "db_type", "db_name"]
            missing_fields = [field for field in required_fields if field not in task_config]
            if missing_fields:
                raise ValueError(
                    f"Fehlende erforderliche Konfigurationsfelder für 'db_to_frappe': {', '.join(missing_fields)}"
                )
            self.query = task_config["query"]
            if not self.key_fields:
                logging.warning(
                    "Keine 'key_fields' definiert. Es können keine Updates durchgeführt werden, nur Inserts."
                )
        elif self.direction == "frappe_to_db":
            required_fields = ["endpoint", "table_name", "mapping", "db_type", "db_name"]
            missing_fields = [field for field in required_fields if field not in task_config]
            if missing_fields:
                raise ValueError(
                    f"Fehlende erforderliche Konfigurationsfelder für 'frappe_to_db': {', '.join(missing_fields)}"
                )
            self.table_name = task_config["table_name"]
            if not self.key_fields:
                logging.warning(
                    "Keine 'key_fields' definiert. Es können keine Updates durchgeführt werden, nur Inserts."
                )
        else:
            raise ValueError(f"Unbekannte Synchronisationsrichtung: {self.direction}")

        # Sicherstellen, dass 'key_fields' eine Liste ist
        if self.key_fields:
            if isinstance(self.key_fields, str):
                self.key_fields = [self.key_fields]
            elif not isinstance(self.key_fields, list):
                raise ValueError("'key_fields' muss ein String oder eine Liste von Strings sein.")
        else:
            self.key_fields = []

    def execute(self):
        if self.direction == "db_to_frappe":
            self.sync_db_to_frappe()
        elif self.direction == "frappe_to_db":
            self.sync_frappe_to_db()
        else:
            logging.error(f"Unbekannte Synchronisationsrichtung: {self.direction}")

    def sync_db_to_frappe(self):
        cursor = self.db_conn.cursor()
        cursor.execute(self.query)
        columns = [column[0] for column in cursor.description]

        for row in cursor.fetchall():
            data = {}
            for frappe_field, db_column in self.mapping.items():
                if db_column in columns:
                    data[frappe_field] = row[columns.index(db_column)]
                else:
                    logging.warning(f"Spalte {db_column} nicht im Ergebnis gefunden.")

            # Überprüfen, ob das Dokument bereits existiert
            filters = []
            for key_field in self.key_fields:
                if key_field in data:
                    filters.append([key_field, "=", data[key_field]])
                else:
                    logging.warning(f"Schlüsselfeld {key_field} nicht in Daten gefunden.")

            if filters:
                # URL-encode die Filter
                import urllib.parse

                filters_str = urllib.parse.quote(str(filters))
                endpoint = f"{self.endpoint}?filters={filters_str}"

                # Suche nach existierendem Dokument
                existing_docs = self.frappe_api.get_data(endpoint)
                if existing_docs and existing_docs.get("data"):
                    # Dokument(e) existieren
                    if self.process_all:
                        # Verarbeite alle gefundenen Dokumente
                        for doc in existing_docs["data"]:
                            doc_name = doc["name"]
                            method = "PUT"
                            doc_endpoint = f"{self.endpoint}/{doc_name}"
                            # Daten an Frappe senden
                            self.frappe_api.send_data(method, doc_endpoint, data)
                        continue  # Gehe zur nächsten Zeile in der Schleife
                    else:
                        # Verarbeite nur das erste gefundene Dokument
                        doc_name = existing_docs["data"][0]["name"]
                        method = "PUT"
                        endpoint = f"{self.endpoint}/{doc_name}"
                else:
                    method = "POST"
                    endpoint = self.endpoint  # Zurücksetzen auf den Endpunkt ohne doc_name
            else:
                method = "POST"
                endpoint = self.endpoint

            # Daten an Frappe senden
            self.frappe_api.send_data(method, endpoint, data)

        cursor.close()

    def sync_frappe_to_db(self):
        table_name = self.table_name

        # Daten von Frappe abrufen
        response = self.frappe_api.get_data(self.endpoint)
        if not response:
            return

        data_list = response.get("data", [])
        cursor = self.db_conn.cursor()

        for data in data_list:
            # Mapping umkehren: Frappe-Felder zu DB-Spalten
            db_data = {}
            for frappe_field, db_column in self.mapping.items():
                if frappe_field in data:
                    db_data[db_column] = data[frappe_field]
                else:
                    logging.warning(f"Feld {frappe_field} nicht in Frappe-Daten gefunden.")

            # Prüfen, ob alle Schlüsselfelder vorhanden sind
            key_values = {}
            missing_keys = False
            for key_field in self.key_fields:
                if key_field in data:
                    key_values[key_field] = data[key_field]
                else:
                    logging.warning(f"Schlüsselfeld {key_field} nicht in Frappe-Daten gefunden.")
                    missing_keys = True

            if missing_keys:
                logging.warning("Nicht alle Schlüsselfelder vorhanden. Überspringe Datensatz.")
                continue  # Zum nächsten Datensatz springen

            # Überprüfen, ob der Datensatz existiert
            where_clause = " AND ".join([f"{key} = ?" for key in key_values.keys()])
            select_sql = f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"
            params = list(key_values.values())
            cursor.execute(select_sql, params)
            exists = cursor.fetchone()[0] > 0

            if exists:
                # Update
                set_clause = ", ".join([f"{col} = ?" for col in db_data.keys()])
                update_sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
                params = list(db_data.values()) + list(key_values.values())
                try:
                    cursor.execute(update_sql, params)
                    self.db_conn.commit()
                    logging.info(f"Datensatz mit Schlüsseln {key_values} erfolgreich aktualisiert.")
                except Exception as e:
                    logging.error(f"Fehler beim Aktualisieren des Datensatzes: {e}")
                    self.db_conn.rollback()
            else:
                # Insert
                placeholders = ", ".join(["?"] * len(db_data))
                columns = ", ".join(db_data.keys())
                insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                try:
                    cursor.execute(insert_sql, list(db_data.values()))
                    self.db_conn.commit()
                    logging.info(f"Datensatz mit Schlüsseln {key_values} erfolgreich eingefügt.")
                except Exception as e:
                    logging.error(f"Fehler beim Einfügen des Datensatzes: {e}")
                    self.db_conn.rollback()

        cursor.close()
