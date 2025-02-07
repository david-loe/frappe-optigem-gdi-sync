import logging

import urllib
from sync.task import SyncTaskBase


class DbToFrappeSyncTask(SyncTaskBase):
    name = "DB -> Frappe"

    def validate_config(self):
        required_fields = ["endpoint", "mapping", "db_name"]
        missing_fields = [field for field in required_fields if getattr(self, field, None) is None]

        if not self.query and not self.table_name:
            missing_fields.append("query oder table_name")  # Mindestens eines muss vorhanden sein.

        if missing_fields:
            raise ValueError(
                f"Fehlende erforderliche Konfigurationsfelder für 'db_to_frappe': {', '.join(missing_fields)}"
            )
        if not self.key_fields:
            logging.warning("Keine 'key_fields' definiert. Es können keine Updates durchgeführt werden, nur Inserts.")

    def sync(self):
        logging.info(f"Starte Ausführung von '{self.name}'.")
        db_records = self.get_db_records()

        for record in db_records:
            data = self.map_db_to_frappe(record)
            filters = self.get_filters_from_data(data)
            if filters:
                # Suche nach existierendem Dokument
                filters_str = urllib.parse.quote(f"[{','.join(filters)}]")
                endpoint = f"{self.endpoint}?filters={filters_str}"
                existing_docs = self.frappe_api.get_data(endpoint)
                if existing_docs and existing_docs.get("data"):
                    # Dokument(e) existieren
                    if self.process_all:
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
        for key_field in self.key_fields:
            if key_field in data:
                filters.append(f'["{key_field}", "=", "{data[key_field]}"]')
            else:
                logging.warning(f"Schlüsselfeld {key_field} nicht in Daten gefunden.")
        return filters
