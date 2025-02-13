import logging
from datetime import datetime

from config import BidirectionalTaskConfig
from sync.task import SyncTaskBase


class BidirectionalSyncTask(SyncTaskBase[BidirectionalTaskConfig]):
    """
    Bidirektionaler Sync zwischen Frappe und einer PostgreSQL-Datenbank.
    Konflikte werden anhand von Änderungstimestamps gelöst:
      - Ist der Frappe-Datensatz neuer, wird die DB aktualisiert.
      - Ist der DB-Datensatz neuer, wird Frappe aktualisiert.
      - Existiert ein Datensatz nur auf einer Seite, wird er (bei create_new=True) eingefügt.
    """

    def sync(self):
        logging.info(f"Starte bidirektionalen Sync: {self.config.name}")

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
                # Der Datensatz existiert in Frappe, aber nicht in der DB.
                # Prüfe, ob der Frappe-Datensatz bereits synchronisiert wurde – er hätte dann das fk_id-Feld gesetzt.
                if frappe_rec.get(self.config.frappe.fk_id_field):
                    logging.info(f"Lösche Frappe-Datensatz {key}")
                    self.delete_frappe_record_by_id(frappe_rec)
                else:
                    logging.info(f"Neuer Frappe-Datensatz {key} gefunden. Einfügen in die DB.")
                    created_db_rec = self.insert_frappe_record_to_db(frappe_rec)
                    if created_db_rec and created_db_rec.get("id"):
                        self.update_frappe_foreign_id(frappe_rec, created_db_rec["id"])

            elif db_rec and not frappe_rec:
                # Der Datensatz existiert in der DB, aber nicht in Frappe.
                # Falls der DB-Datensatz bereits synchronisiert wurde, sollte das fk_id-Feld gesetzt sein.
                if db_rec.get(self.config.db.fk_id_field):
                    logging.info(f"Lösche DB-Datensatz {key}")
                    self.delete_db_record_by_id(db_rec)
                else:
                    logging.info(f"Neuer DB-Datensatz {key} gefunden. Einfügen in Frappe.")
                    created_frappe_doc = self.insert_db_record_to_frappe(db_rec)
                    if created_frappe_doc and created_frappe_doc.get("name"):
                        self.update_db_foreign_id(db_rec, created_frappe_doc["name"])

    def get_modified_timestamp(self, record: dict, source: str) -> datetime:
        """
        Liest den Änderungs-Timestamp aus einem Datensatz.
        source: 'frappe' oder 'db'
        """
        ts_str = None
        if source == "frappe":
            ts_str = record.get(self.config.frappe.modified_field)
        elif source == "db":
            ts_str = record.get(self.config.db.modified_field)
            if ts_str is None:
                ts_str = record.get(self.config.db.fallback_modified_field)
        if ts_str is None:
            return None
        if isinstance(ts_str, datetime):
            return ts_str
        else:
            try:
                return datetime.fromisoformat(ts_str)
            except Exception as e:
                logging.error(source)
                logging.error(f"Fehler beim Parsen des Timestamps '{ts_str}' (type:{type(ts_str)}): {e}")
                return None
