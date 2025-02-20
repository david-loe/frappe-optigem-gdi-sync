from datetime import datetime
import logging

from config import DbToFrappeTaskConfig
from sync.task import SyncTaskBase


class DbToFrappeSyncTask(SyncTaskBase[DbToFrappeTaskConfig]):
    def sync(self, last_sync_date: datetime | None = None):
        db_records = self.get_db_records(last_sync_date)

        for record in db_records:
            data = self.map_db_to_frappe(record)
            filters = self.get_filters_from_data(data)
            if filters:
                # Suche nach existierendem Dokument
                existing_docs = self.frappe_api.get_data(self.config.doc_type, filters=filters)
                if existing_docs and existing_docs.get("data"):
                    # Dokument(e) existieren
                    if self.config.process_all:
                        for doc in existing_docs["data"]:
                            self.update_frappe_record(record, doc["name"])
                    else:
                        self.update_frappe_record(record, existing_docs["data"][0]["name"])
                else:
                    self.insert_db_record_to_frappe(record)
            else:
                self.insert_db_record_to_frappe(record)

    def get_filters_from_data(self, data: dict):
        filters: list[str] = []
        for key_field in self.config.key_fields:
            if key_field in data:
                filters.append(f'["{key_field}", "=", "{data[key_field]}"]')
            else:
                logging.warning(f"Schlüsselfeld {key_field} nicht in Daten gefunden.")
        return filters
