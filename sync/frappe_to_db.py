from datetime import datetime
import logging
from config import FrappeToDbTaskConfig
from sync.task import SyncTaskBase


class FrappeToDbSyncTask(SyncTaskBase[FrappeToDbTaskConfig]):
    def sync(self, last_sync_date_utc: datetime | None = None):
        # Daten von Frappe abrufen
        frappe_records = self.get_frappe_records(last_sync_date_utc)

        for frappe_rec in frappe_records:
            data, key_values = self.split_frappe_in_data_and_keys(frappe_rec)

            # Überprüfen, ob der Datensatz existiert
            where_clause = " AND ".join([f"{key} = ?" for key in key_values.keys()])
            select_sql = f"SELECT COUNT(*) FROM {self.config.table_name} WHERE {where_clause}"
            params = list(key_values.values())
            exists = False
            cursor = self.db_conn.cursor()
            try:
                cursor.execute(select_sql, params)
                exists = cursor.fetchone()[0] > 0
            finally:
                cursor.close()

            if exists:
                self.update_db_record(frappe_rec)
            else:
                self.insert_frappe_record_to_db(frappe_rec)
