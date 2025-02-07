import logging

import urllib
from sync.task import SyncTaskBase


class BidirectionalSyncTask(SyncTaskBase):
    name = "Frappe <-> DB"

    def validate_config(self):
        required_fields = ["endpoint", "mapping", "db_name", "table_name", "query", "key_fields"]
        missing_fields = [field for field in required_fields if not hasattr(self, field)]
        if missing_fields:
            raise ValueError(
                f"Fehlende erforderliche Konfigurationsfelder für 'db_to_frappe': {', '.join(missing_fields)}"
            )

    def sync(self):
        logging.info(f"Starte Ausführung von '{self.name}'.")
