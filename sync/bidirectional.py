import logging
from datetime import datetime
from typing import Literal

from config import BidirectionalTaskConfig
from sync.task import SyncTaskBase


class BidirectionalSyncTask(SyncTaskBase[BidirectionalTaskConfig]):
    def sync(self, last_sync_date: datetime | None = None):
        frappe_dict = self.get_frappe_key_record_dict(last_sync_date)
        db_dict = self.get_db_key_record_dict(last_sync_date)

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
                    self.update_frappe_record(db_rec, frappe_rec[self.config.frappe.id_field])
                else:
                    logging.info(f"Datensatz {key} ist synchronisiert.")

            elif frappe_rec and not db_rec:
                # Der Datensatz existiert in Frappe, aber nicht in der DB.
                # Prüfe, ob der Frappe-Datensatz bereits synchronisiert wurde – er hätte dann das fk_id-Feld gesetzt.
                if frappe_rec.get(self.config.frappe.fk_id_field):
                    self.delete_frappe_record(frappe_rec)
                else:
                    logging.info(f"Neuer Frappe-Datensatz {key} gefunden. Einfügen in die DB.")
                    created_db_rec = self.insert_frappe_record_to_db(frappe_rec)
                    if created_db_rec and created_db_rec.get(self.config.db.id_field):
                        self.update_frappe_foreign_id(frappe_rec, created_db_rec[self.config.db.id_field])

            elif db_rec and not frappe_rec:
                # Der Datensatz existiert in der DB, aber nicht in Frappe.
                # Falls der DB-Datensatz bereits synchronisiert wurde, sollte das fk_id-Feld gesetzt sein.
                if db_rec.get(self.config.db.fk_id_field):
                    self.delete_db_record(db_rec)
                else:
                    logging.info(f"Neuer DB-Datensatz {key} gefunden. Einfügen in Frappe.")
                    created_frappe_doc = self.insert_db_record_to_frappe(db_rec)
                    if created_frappe_doc and created_frappe_doc.get(self.config.frappe.id_field):
                        self.update_db_foreign_id(db_rec, created_frappe_doc[self.config.frappe.id_field])

    def get_modified_timestamp(self, record: dict, source: Literal["frappe", "db"]) -> datetime | None:
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

    def update_db_foreign_id(self, db_rec: dict, foreign_id: str):
        set_clause = f"{self.config.db.fk_id_field} = ?"
        where_clause = f"{self.config.db.id_field} = ?"
        sql = f"UPDATE {self.config.table_name} SET {set_clause} WHERE {where_clause}"
        params = [foreign_id, db_rec.get(self.config.db.id_field)]
        self.execute_query(sql, params, f"DB-Datensatz wurde aktualisiert.")

    def update_frappe_foreign_id(self, frappe_rec: dict, foreign_id: str):
        data = {}
        data[self.config.frappe.id_field] = foreign_id
        return self.frappe_api.update_data(self.config.doc_type, frappe_rec.get(self.config.frappe.id_field), data).get(
            "data"
        )

    def delete_frappe_record(self, frappe_rec: dict):
        if self.config.delete:
            self.frappe_api.delete(self.config.doc_type, frappe_rec[self.config.frappe.id_field])

    def delete_db_record(self, db_rec: dict):
        if self.config.delete:
            sql = f"DELETE FROM {self.config.table_name} WHERE {self.config.db.id_field} = ?"
            self.execute_query(
                sql,
                [db_rec[self.config.db.id_field]],
                f"DB-Datensatz {db_rec[self.config.db.id_field]} wurde gelöscht.",
            )
