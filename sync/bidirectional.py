import logging
from datetime import datetime, timedelta
from typing import Literal

from config import BidirectionalTaskConfig
from sync.task import SyncTaskBase


class BidirectionalSyncTask(SyncTaskBase[BidirectionalTaskConfig]):
    def sync(self, last_sync_date_utc: datetime | None = None):
        frappe_dict = self.get_frappe_key_record_dict(self.get_frappe_records(last_sync_date_utc))
        db_dict = self.get_db_key_record_dict(self.get_db_records(last_sync_date_utc))

        # check for same types in key
        if len(frappe_dict) > 0 and len(db_dict) > 0:
            first_frappe_key = next(iter(frappe_dict))
            first_db_key = next(iter(db_dict))
            if not self.compare_key_tuple_structure(first_frappe_key, first_db_key):
                raise ValueError("Die Schlüssel-Tupel haben einen unterschiedlichen Typaufbau!")

        # Falls nur die letzten Änderungen synchronisiert werden, muss geprüft werden, ob die Gegenseite nicht doch Einträge enthält
        if last_sync_date_utc:
            missing_db_keys = frappe_dict.keys() - db_dict.keys()
            missing_db_ids = [
                frappe_dict[key].get(self.config.frappe.fk_id_field)
                for key in missing_db_keys
                if frappe_dict[key].get(self.config.frappe.fk_id_field)
            ]
            missing_frappe_keys = db_dict.keys() - frappe_dict.keys()
            missing_frappe_ids = [
                db_dict[key].get(self.config.db.fk_id_field)
                for key in missing_frappe_keys
                if db_dict[key].get(self.config.db.fk_id_field)
            ]
            if missing_frappe_ids:
                additional_frappe_records = self.get_frappe_records_by_ids(missing_frappe_ids)
                frappe_dict.update(self.get_frappe_key_record_dict(additional_frappe_records))
            if missing_db_ids:
                additional_db_records = self.get_db_records_by_ids(missing_db_ids)
                db_dict.update(self.get_db_key_record_dict(additional_db_records))

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
                frappe_newer = compare_datetimes(
                    frappe_ts, db_ts, self.config.datetime_comparison_accuracy_milliseconds
                )
                if frappe_newer > 0:
                    logging.info(f"Konflikt für Schlüssel {key}: Frappe ist aktueller. Aktualisiere DB.")
                    self.update_db_record(frappe_rec)
                elif frappe_newer < 0:
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
        timestamp = None
        if source == "frappe":
            for modified_field in self.config.frappe.modified_fields:
                timestamp = record.get(modified_field)
                if timestamp:
                    timestamp = timestamp - self.frappe_tz_delta
                    break
        elif source == "db":
            for modified_field in self.config.db.modified_fields:
                timestamp = record.get(modified_field)
                if timestamp:
                    timestamp = timestamp - self.db_tz_delta
                    break
        return timestamp

    def update_db_foreign_id(self, db_rec: dict, foreign_id: str):
        set_clause = f"{self.esc_db_col(self.config.db.fk_id_field)} = ?"
        where_clause = f"{self.esc_db_col(self.config.db.id_field)} = ?"
        sql = f"UPDATE {self.config.table_name} SET {set_clause} WHERE {where_clause}"
        params = [foreign_id, db_rec.get(self.config.db.id_field)]
        self.execute_query(sql, params, f"DB-Datensatz wurde aktualisiert.")

    def update_frappe_foreign_id(self, frappe_rec: dict, foreign_id: str):
        data = {}
        data[self.config.frappe.fk_id_field] = foreign_id
        res = self.frappe_api.update_data(self.config.doc_type, frappe_rec.get(self.config.frappe.id_field), data)
        if res:
            return res.get("data")

    def delete_frappe_record(self, frappe_rec: dict):
        if self.config.delete:
            self.frappe_api.delete(self.config.doc_type, frappe_rec[self.config.frappe.id_field])

    def delete_db_record(self, db_rec: dict):
        if self.config.delete:
            sql = f"DELETE FROM {self.config.table_name} WHERE {self.esc_db_col(self.config.db.id_field)} = ?"
            self.execute_query(
                sql,
                [db_rec[self.config.db.id_field]],
                f"DB-Datensatz {db_rec[self.config.db.id_field]} wurde gelöscht.",
            )

    def compare_key_tuple_structure(self, frappe_key: tuple, db_key: tuple) -> bool:
        if len(frappe_key) != len(db_key):
            logging.debug(
                f"Unterschiedliche Länge: frappe hat {len(frappe_key)} Elemente, key2 hat {len(db_key)} Elemente."
            )
            return False
        for key_name, elem1, elem2 in zip(self.config.key_fields, frappe_key, db_key):
            logging.debug(f"Key Type {key_name}: frappe - {type(elem1).__name__} |  {type(elem2).__name__} - db")
            if type(elem1) != type(elem2) and type(elem1) != type(None) and type(elem2) != type(None):
                return False
        return True


def compare_datetimes(dt1: datetime, dt2: datetime, tolerance_ms: int):
    # Berechne die Differenz zwischen den beiden Datumswerten
    delta = dt1 - dt2
    # Erstelle ein timedelta-Objekt, das der gewünschten Toleranz entspricht
    tolerance = timedelta(milliseconds=tolerance_ms)

    # Wenn der absolute Unterschied innerhalb der Toleranz liegt, gelten sie als gleich
    if abs(delta) <= tolerance:
        return 0  # 0 bedeutet "gleich"
    elif delta > tolerance:
        return 1  # 1 bedeutet "dt1 ist größer als dt2"
    else:
        return -1  # -1 bedeutet "dt1 ist kleiner als dt2"
