import logging
from datetime import datetime

from sync.task import SyncTaskBase


class BidirectionalSyncTask(SyncTaskBase):
    """
    Bidirektionaler Sync zwischen Frappe und einer PostgreSQL-Datenbank.
    Konflikte werden anhand von Änderungstimestamps gelöst:
      - Ist der Frappe-Datensatz neuer, wird die DB aktualisiert.
      - Ist der DB-Datensatz neuer, wird Frappe aktualisiert.
      - Existiert ein Datensatz nur auf einer Seite, wird er (bei create_new=True) eingefügt.
    """

    name = "Bidirectional Sync"

    def validate_config(self):
        required_fields = [
            "endpoint",
            "mapping",
            "db_name",
            "table_name",
            "key_fields",
            "frappe_modified_field",
            "db_modified_field",
        ]
        missing_fields = [field for field in required_fields if getattr(self, field, None) is None]
        if missing_fields:
            raise ValueError(
                f"Fehlende erforderliche Konfigurationsfelder für bidirektionalen Sync: {', '.join(missing_fields)}"
            )
        if len(self.key_fields) == 0:
            raise ValueError("'key_fields' muss gesetzt sein für bidirektionalen Sync.")
        if self.query is None:
            logging.info(f"Es werden alle Einträge der Tabelle '{self.table_name}' synchronisiert")

    def sync(self):
        logging.info(f"Starte bidirektionalen Sync: {self.name}")

        frappe_dict = self.get_frappe_key_record_dict()
        db_dict = self.get_db_key_record_dict()

        # Alle vorhandenen Schlüssel zusammenführen
        all_keys = set(frappe_dict.keys()).union(db_dict.keys())
        for key in all_keys:
            frappe_rec = frappe_dict.get(key)
            db_rec = db_dict.get(key)
            if frappe_rec and db_rec:
                # Datensatz existiert auf beiden Seiten – Konfliktmanagement anhand der Timestamps
                frappe_ts = self.get_modified_timestamp(frappe_rec, source="frappe")
                db_ts = self.get_modified_timestamp(db_rec, source="db")
                if frappe_ts is None or db_ts is None:
                    logging.warning(f"Fehlender Timestamp für Schlüssel {key}. Konflikt wird übersprungen.")
                    continue
                if frappe_ts > db_ts:
                    logging.info(f"Konflikt für Schlüssel {key}: Frappe ist aktueller. Aktualisiere DB.")
                    self.update_db_record(frappe_rec)
                elif db_ts > frappe_ts:
                    logging.info(f"Konflikt für Schlüssel {key}: DB ist aktueller. Aktualisiere Frappe.")
                    self.update_frappe_record(db_rec, frappe_rec["name"])
                else:
                    logging.info(f"Datensatz {key} ist synchronisiert.")
            elif frappe_rec and not db_rec:
                logging.info(f"Datensatz {key} existiert nur in Frappe. Einfügen in DB.")
                self.insert_frappe_record_to_db(frappe_rec)
            elif db_rec and not frappe_rec:
                logging.info(f"Datensatz {key} existiert nur in DB. Einfügen in Frappe.")
                self.insert_db_record_to_frappe(db_rec)

    def get_modified_timestamp(self, record: dict, source: str) -> datetime:
        """
        Liest den Änderungs-Timestamp aus einem Datensatz.
        source: 'frappe' oder 'db'
        """
        ts_str = None
        if source == "frappe":
            ts_str = record.get(self.frappe_modified_field)
        elif source == "db":
            ts_str = record.get(self.db_modified_field)
        if ts_str is None:
            return None
        try:
            return datetime.fromisoformat(ts_str)
        except Exception as e:
            logging.error(f"Fehler beim Parsen des Timestamps '{ts_str}': {e}")
            return None
