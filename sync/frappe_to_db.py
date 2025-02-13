import logging
from config import FrappeToDbTaskConfig
from sync.task import SyncTaskBase


class FrappeToDbSyncTask(SyncTaskBase[FrappeToDbTaskConfig]):
    def sync(self):
        logging.info(f"Starte Ausführung von '{self.config.name}'.")

        # Daten von Frappe abrufen
        frappe_records = self.get_frappe_records()

        for frappe_rec in frappe_records:
            key_values, missing_keys = self.get_key_values_from_data(frappe_rec)

            if missing_keys:
                logging.warning("Nicht alle Schlüsselfelder vorhanden. Überspringe Datensatz.")
                continue  # Zum nächsten Datensatz springen

            # Überprüfen, ob der Datensatz existiert
            where_clause = " AND ".join([f"{key} = ?" for key in key_values.keys()])
            select_sql = f"SELECT COUNT(*) FROM {self.config.table_name} WHERE {where_clause}"
            params = list(key_values.values())
            exists = False
            with self.db_conn.cursor() as cursor:
                cursor.execute(select_sql, params)
                exists = cursor.fetchone()[0] > 0
            if exists:
                self.update_db_record(frappe_rec)

            elif self.create_new:
                # Insert
                self.insert_frappe_record_to_db(frappe_rec)

    def get_key_values_from_data(self, data: dict):
        key_values = {}
        missing_keys = False
        for key_field in self.config.key_fields:
            if key_field in data:
                key_values[key_field] = data[key_field]
            else:
                logging.warning(f"Schlüsselfeld {key_field} nicht in Frappe-Daten gefunden.")
                missing_keys = True
                continue
        return key_values, missing_keys
