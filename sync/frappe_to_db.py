import logging
from api.database import format_query
from sync.task import SyncTaskBase


class FrappeToDbSyncTask(SyncTaskBase):
    name = "Frappe -> DB"

    def validate_config(self):
        required_fields = ["endpoint", "table_name", "mapping", "db_name"]
        missing_fields = [field for field in required_fields if not hasattr(self, field)]
        if missing_fields:
            raise ValueError(
                f"Fehlende erforderliche Konfigurationsfelder für 'frappe_to_db': {', '.join(missing_fields)}"
            )
        if not self.key_fields:
            logging.warning("Keine 'key_fields' definiert. Es können keine Updates durchgeführt werden, nur Inserts.")

    def sync(self):
        logging.info(f"Starte Ausführung von '{self.name}'.")

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
            select_sql = f"SELECT COUNT(*) FROM {self.table_name} WHERE {where_clause}"
            params = list(key_values.values())
            cursor.execute(select_sql, params)
            exists = cursor.fetchone()[0] > 0

            if exists:
                # Update
                set_clause = ", ".join([f"{col} = ?" for col in db_data.keys()])
                update_sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {where_clause}"
                params = list(db_data.values()) + list(key_values.values())
                if self.dry_run:
                    logging.info(
                        f"""DRY_RUN: {self.db_name}
                            {format_query(update_sql, params)}"""
                    )
                else:
                    try:
                        cursor.execute(update_sql, params)
                        self.db_conn.commit()
                        logging.info(f"Datensatz mit Schlüsseln {key_values} erfolgreich aktualisiert.")
                    except Exception as e:
                        logging.error(f"Fehler beim Aktualisieren des Datensatzes: {e}")
                        self.db_conn.rollback()
            elif self.create_new:
                # Insert
                placeholders = ", ".join(["?"] * len(db_data))
                columns = ", ".join(db_data.keys())
                insert_sql = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
                if self.dry_run:
                    logging.info(
                        f"""DRY_RUN: {self.db_name}
                            {format_query(update_sql, params)}"""
                    )
                else:
                    try:
                        cursor.execute(insert_sql, list(db_data.values()))
                        self.db_conn.commit()
                        logging.info(f"Datensatz mit Schlüsseln {key_values} erfolgreich eingefügt.")
                    except Exception as e:
                        logging.error(f"Fehler beim Einfügen des Datensatzes: {e}")
                        self.db_conn.rollback()

        cursor.close()
