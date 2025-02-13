import logging

import urllib
from config import DbToFrappeTaskConfig
from sync.task import SyncTaskBase


class DbToFrappeSyncTask(SyncTaskBase[DbToFrappeTaskConfig]):
    def sync(self):
        logging.info(f"Starte Ausführung von '{self.config.name}'.")
        db_records = self.get_db_records()

        for record in db_records:
            data = self.map_db_to_frappe(record)
            filters = self.get_filters_from_data(data)
            if filters:
                # Suche nach existierendem Dokument
                filters_str = urllib.parse.quote(f"[{','.join(filters)}]")
                endpoint = f"{self.config.endpoint}?filters={filters_str}"
                existing_docs = self.frappe_api.get_data(endpoint)
                if existing_docs and existing_docs.get("data"):
                    # Dokument(e) existieren
                    if self.config.process_all:
                        for doc in existing_docs["data"]:
                            self.update_frappe_record(record, doc["name"])
                    else:
                        self.update_frappe_record(record, existing_docs["data"][0]["name"])
                elif self.create_new:
                    self.insert_db_record_to_frappe(record)
            elif self.create_new:
                self.insert_db_record_to_frappe(record)

    def get_filters_from_data(self, data: dict):
        filters: list[str] = []
        for key_field in self.config.key_fields:
            if key_field in data:
                filters.append(f'["{key_field}", "=", "{data[key_field]}"]')
            else:
                logging.warning(f"Schlüsselfeld {key_field} nicht in Daten gefunden.")
        return filters
