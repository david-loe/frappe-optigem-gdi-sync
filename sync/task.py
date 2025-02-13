from abc import ABC, abstractmethod
import datetime
import logging
from typing import TypeVar, Generic
from api.database import DatabaseConnection, format_query
from api.frappe import FrappeAPI
from config import TaskConfig

T = TypeVar("T", bound=TaskConfig)


class SyncTaskBase(Generic[T], ABC):
    def __init__(self, task_config: T, db_conn: DatabaseConnection, frappe_api: FrappeAPI, dry_run: bool):
        self.config = task_config
        self.frappe_api = frappe_api
        self.dry_run = dry_run
        self.db_conn = db_conn.get_connection(self.config.db_name)

    @abstractmethod
    def sync(self):
        """Führt die Synchronisation aus."""
        pass

    def execute_query(self, sql: str, params: list, success_msg: str):
        if self.dry_run:
            logging.info(f"DRY_RUN: {self.config.db_name}\n{format_query(sql, params)}")
            return
        try:
            logging.debug(
                f"""Anfrage an {self.config.db_name}
                    {format_query(sql, params)}"""
            )
            with self.db_conn.cursor() as cursor:
                cursor.execute(sql, params)
            self.db_conn.commit()
            logging.info(success_msg)
        except Exception as e:
            logging.error(f"Fehler bei der Ausführung von SQL: {e}")
            self.db_conn.rollback()

    def _execute_select_query(self, sql: str, params: list = []):
        db_records: list[dict[str, any]] = []
        with self.db_conn.cursor() as cursor:
            logging.debug(
                f"""Anfrage an {self.config.db_name}
                    {format_query(sql, params)}"""
            )
            cursor.execute(sql, params)
            db_columns = [desc[0] for desc in cursor.description]
            for row in cursor.fetchall():
                rec = dict(zip(db_columns, row))
                db_records.append(rec)
        return db_records

    def map_frappe_to_db(self, record: dict, warns=True) -> dict:
        """
        Übersetzt einen Frappe-Datensatz in ein DB-Datenformat anhand des Mapping.
        """
        db_data = {}
        for frappe_field, db_column in self.config.mapping.items():
            if frappe_field in record:
                value = record[frappe_field]
                if value is not None:
                    db_data[db_column] = value
            elif warns:
                logging.warning(f"Feld '{frappe_field}' fehlt im Frappe-Datensatz {record}.")
        return db_data

    def map_db_to_frappe(self, record: dict, warns=True) -> dict:
        """
        Übersetzt einen DB-Datensatz in ein Frappe-Datenformat anhand des inversen Mapping.
        """
        frappe_data = {}
        for frappe_field, db_column in self.config.mapping.items():
            if db_column in record:
                value = record[db_column]
                if value is not None:
                    frappe_data[frappe_field] = value
            elif warns:
                logging.warning(f"Spalte '{db_column}' fehlt im DB-Datensatz {record}.")
        return frappe_data

    def split_frappe_in_data_and_keys(self, frappe_rec: dict):
        keys = {}
        data = {}
        for k, v in frappe_rec.items():
            if k in self.config.key_fields:
                keys[k] = v
            else:
                data[k] = v
        return data, keys

    def get_frappe_records(self) -> list:
        """
        Frappe-Datensätze abrufen
        """
        frappe_response = self.frappe_api.get_all_data(self.config.endpoint)
        records = frappe_response.get("data", [])
        for rec in records:
            for field in self.config.frappe.datetime_fields:
                if field in rec and isinstance(rec[field], str):
                    try:
                        rec[field] = datetime.datetime.fromisoformat(rec[field])
                    except ValueError:
                        # Falls der String kein gültiges ISO-Datum ist, bleibt der Wert unverändert.
                        pass
        return records

    def get_frappe_key_record_dict(self):
        frappe_records = self.get_frappe_records()
        frappe_dict: dict[tuple, dict[str, any]] = {}
        for rec in frappe_records:
            key = self.extract_key_from_frappe(rec)
            frappe_dict[key] = rec
        return frappe_dict

    def get_db_records(self):
        """
        DB-Datensätze abrufen
        """
        select_sql = f"SELECT * FROM {self.config.table_name}"
        if self.config.query:
            select_sql = self.config.query
        return self._execute_select_query(select_sql)

    def get_db_key_record_dict(self):
        db_records = self.get_db_records()
        db_dict: dict[tuple, dict[str, any]] = {}
        for rec in db_records:
            key = self.extract_key_from_db(rec)
            db_dict[key] = rec
        return db_dict

    def extract_key_from_frappe(self, record: dict) -> tuple:
        """
        Erzeugt einen Schlüssel (tuple) aus einem Frappe-Datensatz basierend auf den key_fields.
        """
        return tuple(record.get(field) for field in self.config.key_fields)

    def extract_key_from_db(self, record: dict) -> tuple:
        """
        Erzeugt einen Schlüssel (tuple) aus einem DB-Datensatz. Dabei wird die Mapping-Übersetzung angewandt.
        """
        key_values = []
        for field in self.config.key_fields:
            key_values.append(record.get(self.config.mapping[field]))
        return tuple(key_values)

    def insert_db_record_to_frappe(self, db_rec: dict):
        """
        Fügt einen neuen Datensatz in Frappe ein, basierend auf den Daten aus der DB.
        """
        if self.config.create_new:
            frappe_data = self.map_db_to_frappe(db_rec)
            return self.frappe_api.send_data("POST", self.config.endpoint, frappe_data).get("data")

    def update_frappe_record(self, db_rec: dict, frappe_doc_name: str):
        """
        Aktualisiert einen vorhandenen Frappe-Datensatz mit den Werten aus dem DB-Datensatz.
        Es wird davon ausgegangen, dass der Frappe-Datensatz ein eindeutiges 'name'-Feld besitzt.
        """
        frappe_rec = self.map_db_to_frappe(db_rec)
        frappe_data, frappe_keys = self.split_frappe_in_data_and_keys(frappe_rec)
        endpoint = f"{self.config.endpoint}/{frappe_doc_name}"
        return self.frappe_api.send_data("PUT", endpoint, frappe_data).get("data")

    def update_db_record(self, frappe_rec: dict):
        """
        Aktualisiert einen vorhandenen DB-Datensatz mit den Werten aus dem Frappe-Datensatz.
        """
        frappe_rec_data, frappe_rec_keys = self.split_frappe_in_data_and_keys(frappe_rec)
        db_data = self.map_frappe_to_db(frappe_rec_data, warns=False)
        db_keys = self.map_frappe_to_db(frappe_rec_keys, warns=False)
        where_clause = " AND ".join([f"{k} = ?" for k in db_keys.keys()])
        set_clause = ", ".join([f"{col} = ?" for col in db_data.keys()])
        sql = f"UPDATE {self.config.table_name} SET {set_clause} WHERE {where_clause}"
        params = list(db_data.values()) + list(db_keys.values())
        self.execute_query(sql, params, f"DB-Datensatz wurde aktualisiert.")

        sql_select = f"SELECT * FROM {self.config.table_name} WHERE {where_clause}"
        results = self._execute_select_query(sql_select, list(db_keys.values()))
        if len(results) == 0:
            logging.warning(f"DB-Datensatz konnte nach UPDATE nicht gefunden werden: {db_keys}")
            return None
        elif len(results) == 1:
            return results[0]
        else:
            logging.warning(f"Nach UPDATE konnten mehrere DB-Datensätze gefunden werden: {db_keys}")
            return results[0]

    def insert_frappe_record_to_db(self, frappe_rec: dict):
        """
        Fügt einen neuen Datensatz in die DB ein, basierend auf den Daten aus Frappe.
        """
        if self.config.create_new:
            db_data = self.map_frappe_to_db(frappe_rec)
            columns = ", ".join(db_data.keys())
            placeholders = ", ".join(["?"] * len(db_data))
            sql = f"INSERT INTO {self.config.table_name} ({columns}) VALUES ({placeholders})"
            params = list(db_data.values())
            self.execute_query(sql, params, f"Neuer DB-Datensatz wurde eingefügt.")

            where_clause = " AND ".join([f"{k} = ?" for k in db_data.keys()])
            sql_select = f"SELECT * FROM {self.config.table_name} WHERE {where_clause}"
            results = self._execute_select_query(sql_select, list(db_data.values()))
            if len(results) == 0:
                logging.warning(f"DB-Datensatz konnte nach UPDATE nicht gefunden werden: {db_data}")
                return None
            elif len(results) == 1:
                return results[0]
            else:
                logging.warning(f"Nach UPDATE konnten mehrere DB-Datensätze gefunden werden: {db_data}")
                return results[0]
