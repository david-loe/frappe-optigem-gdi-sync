from abc import ABC, abstractmethod
import logging
from typing import Any, Literal
from api.database import DatabaseConnection, format_query
from api.frappe import FrappeAPI


class SyncTaskBase(ABC):
    def __init__(self, task_config: dict[str, Any], db_conn: DatabaseConnection, frappe_api: FrappeAPI, dry_run: bool):
        self.task_config = task_config
        self.frappe_api = frappe_api
        self.dry_run = dry_run
        self.load_config()
        self.validate_config()
        self.db_conn = db_conn.get_connection(self.db_name)

    def load_config(self):
        self.name: str = self.task_config.get("name")
        self.endpoint: str = self.task_config.get("endpoint")
        self.mapping: dict[str, str] = self.task_config.get("mapping")
        self.db_name: str = self.task_config.get("db_name")
        self.direction: Literal["db_to_frappe", "frappe_to_db"] = self.task_config.get("direction", "db_to_frappe")
        self.key_fields: list[str] = self.task_config.get("key_fields")
        self.process_all: bool = self.task_config.get("process_all", False)
        self.create_new: bool = self.task_config.get("create_new", False)
        self.query: str = self.task_config.get("query")
        self.table_name: str = self.task_config.get("table_name")
        self.frappe_modified_field: str = self.task_config.get("frappe_modified_field")
        self.db_modified_field: str = self.task_config.get("db_modified_field")

        # Sicherstellen, dass 'key_fields' eine Liste ist
        if self.key_fields:
            if isinstance(self.key_fields, str):
                self.key_fields = [self.key_fields]
            elif not isinstance(self.key_fields, list):
                raise ValueError("'key_fields' muss ein String oder eine Liste von Strings sein.")
        else:
            self.key_fields = []

    @abstractmethod
    def validate_config(self):
        """Überprüft, ob die notwendige Konfiguration vorhanden ist."""
        pass

    @abstractmethod
    def sync(self):
        """Führt die Synchronisation aus."""
        pass

    def execute_query(self, sql: str, params: list, success_msg: str):
        if self.dry_run:
            logging.info(f"DRY_RUN: {self.db_name}\n{format_query(sql, params)}")
            return
        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute(sql, params)
            self.db_conn.commit()
            logging.info(success_msg)
        except Exception as e:
            logging.error(f"Fehler bei der Ausführung von SQL: {e}")
            self.db_conn.rollback()

    def map_frappe_to_db(self, record: dict) -> dict:
        """
        Übersetzt einen Frappe-Datensatz in ein DB-Datenformat anhand des Mapping.
        """
        db_data = {}
        for frappe_field, db_column in self.mapping.items():
            if frappe_field in record:
                db_data[db_column] = record[frappe_field]
            else:
                logging.warning(f"Feld '{frappe_field}' fehlt im Frappe-Datensatz {record}.")
        return db_data

    def map_db_to_frappe(self, record: dict) -> dict:
        """
        Übersetzt einen DB-Datensatz in ein Frappe-Datenformat anhand des inversen Mapping.
        """
        frappe_data = {}
        for frappe_field, db_column in self.mapping.items():
            if db_column in record:
                frappe_data[frappe_field] = record[db_column]
            else:
                logging.warning(f"Spalte '{db_column}' fehlt im DB-Datensatz {record}.")
        return frappe_data

    def get_frappe_records(self) -> list:
        """
        Frappe-Datensätze abrufen
        """
        frappe_response = self.frappe_api.get_data(self.endpoint)
        if not frappe_response:
            logging.error("Keine Daten von Frappe erhalten.")
            return []
        return frappe_response.get("data", [])

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
        db_records = []
        with self.db_conn.cursor() as cursor:
            select_sql = f"SELECT * FROM {self.table_name}"
            if self.query:
                select_sql = self.query
            logging.debug(
                f"""Anfrage an {self.db_name}
                    {select_sql}"""
            )
            cursor.execute(select_sql)
            db_columns = [desc[0] for desc in cursor.description]
            for row in cursor.fetchall():
                rec = dict(zip(db_columns, row))
                db_records.append(rec)
        return db_records

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
        return tuple(record.get(field) for field in self.key_fields)

    def extract_key_from_db(self, record: dict) -> tuple:
        """
        Erzeugt einen Schlüssel (tuple) aus einem DB-Datensatz. Dabei wird die Mapping-Übersetzung angewandt.
        """
        key_values = []
        for field in self.key_fields:
            key_values.append(record.get(self.mapping[field]))
        return tuple(key_values)

    def insert_db_record_to_frappe(self, db_rec: dict):
        """
        Fügt einen neuen Datensatz in Frappe ein, basierend auf den Daten aus der DB.
        """
        frappe_data = self.map_db_to_frappe(db_rec)
        self.frappe_api.send_data("POST", self.endpoint, frappe_data)

    def update_frappe_record(self, db_rec: dict, frappe_doc_name: str):
        """
        Aktualisiert einen vorhandenen Frappe-Datensatz mit den Werten aus dem DB-Datensatz.
        Es wird davon ausgegangen, dass der Frappe-Datensatz ein eindeutiges 'name'-Feld besitzt.
        """
        frappe_data = self.map_db_to_frappe(db_rec)
        endpoint = f"{self.endpoint}/{frappe_doc_name}"
        self.frappe_api.send_data("PUT", endpoint, frappe_data)

    def update_db_record(self, frappe_rec: dict):
        """
        Aktualisiert einen vorhandenen DB-Datensatz mit den Werten aus dem Frappe-Datensatz.
        """
        db_data = self.map_frappe_to_db(frappe_rec)
        where_clause = " AND ".join([f"{self.mapping[field]} = ?" for field in self.key_fields])
        set_clause = ", ".join([f"{col} = ?" for col in db_data.keys()])
        sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {where_clause}"
        params = list(db_data.values()) + [db_data[self.mapping[field]] for field in self.key_fields]
        self.execute_query(sql, params, f"DB-Datensatz wurde aktualisiert.")

    def insert_frappe_record_to_db(self, frappe_rec: dict):
        """
        Fügt einen neuen Datensatz in die DB ein, basierend auf den Daten aus Frappe.
        """
        db_data = self.map_frappe_to_db(frappe_rec)
        columns = ", ".join(db_data.keys())
        placeholders = ", ".join(["?"] * len(db_data))
        sql = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
        params = list(db_data.values())
        self.execute_query(sql, params, f"Neuer DB-Datensatz wurde eingefügt.")
