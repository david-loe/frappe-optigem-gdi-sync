from abc import ABC, abstractmethod
from datetime import datetime
import json
import logging
from typing import TypeVar, Generic
from api.database import DatabaseConnection, format_query, get_time_zone
from api.frappe import FrappeAPI
from config import TaskConfig

T = TypeVar("T", bound=TaskConfig)


class SyncTaskBase(Generic[T], ABC):
    def __init__(
        self, task_name: str, task_config: T, db_conn: DatabaseConnection, frappe_api: FrappeAPI, dry_run: bool
    ):
        self.name = task_name
        self.config = task_config
        self.frappe_api = frappe_api
        self.dry_run = dry_run
        self.db_conn = db_conn.get_connection(self.config.db_name)
        self.esc_db_col = db_conn.get_escape_identifier_fn(self.config.db_name)
        self.frappe_tz_delta = frappe_api.tz_delta
        self.db_tz_delta = get_time_zone(self.db_conn)

    @abstractmethod
    def sync(self, last_sync_date_utc: datetime | None = None):
        """Führt die Synchronisation aus."""
        pass

    def execute_query(self, sql: str, params: list, success_msg: str):
        if self.dry_run:
            logging.info(f"DRY_RUN: {self.config.db_name}\n{format_query(sql, params)}")
            return
        logging.debug(f"Anfrage an {self.config.db_name}\n{format_query(sql, params)}")
        cursor = self.db_conn.cursor()
        try:
            cursor.execute(sql, params)
            self.db_conn.commit()
            logging.info(success_msg)
        except Exception as e:
            logging.error(f"Fehler beim Ausführen der Query '{format_query(sql, params)}'\n{e}")
            self.db_conn.rollback()
        finally:
            cursor.close()

    def _execute_select_query(self, sql: str, params: list = []):
        db_records: list[dict[str, any]] = []
        logging.debug(f"""Anfrage an {self.config.db_name}\n{format_query(sql, params)}""")
        cursor = self.db_conn.cursor()
        try:
            cursor.execute(sql, params)
            db_columns = [desc[0] for desc in cursor.description]
            for row in cursor.fetchall():
                rec = dict(zip(db_columns, row))
                db_records.append(rec)
        except Exception as e:
            logging.error(f"Fehler beim Ausführen der Query '{format_query(sql, params)}'\n{e}")
            self.db_conn.rollback()
        finally:
            cursor.close()
        logging.debug(f"Insgesamt {len(db_records)} Datensätze gefunden.")
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
                    # harmonize time zones
                    if (
                        frappe_field == self.config.frappe.modified_field
                        and db_column == self.config.db.modified_field
                        and isinstance(value, datetime)
                    ):
                        value = value - self.frappe_tz_delta + self.db_tz_delta

                    # use value_mapping
                    if frappe_field in self.config.value_mapping:
                        found = False
                        for f_v, db_v in self.config.value_mapping[frappe_field].items():
                            if f_v == value:
                                value = db_v
                                found = True
                                break
                        if not found and self.config.use_strict_value_mapping:
                            logging.warning(
                                f"Kein Wert in value_mapping gefunden für frappe-Feld '{frappe_field}' und Wert '{value}'."
                            )
                            continue  # skip value
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
                    # use value_mapping
                    if frappe_field in self.config.value_mapping:
                        found = False
                        for f_v, db_v in self.config.value_mapping[frappe_field].items():
                            if db_v == value:
                                value = f_v
                                found = True
                                break
                        if not found and self.config.use_strict_value_mapping:
                            logging.warning(
                                f"Kein Wert in value_mapping gefunden für DB-Feld '{db_column}' und Wert '{value}'."
                            )
                            continue  # skip value

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

    def _cast_frappe_record(self, record: dict):
        for field in self.config.frappe.datetime_fields:
            if field in record and isinstance(record[field], str):
                if not record[field]:
                    record[field] = None
                    continue
                try:
                    record[field] = datetime.fromisoformat(record[field])
                except ValueError:
                    # Falls der String kein gültiges ISO-Datum ist, bleibt der Wert unverändert.
                    pass
        for field in self.config.frappe.int_fields:
            if field in record and isinstance(record[field], str):
                if not record[field]:
                    record[field] = None
                    continue
                try:
                    record[field] = int(record[field])
                except ValueError:
                    # Falls der String kein gültiges ISO-Datum ist, bleibt der Wert unverändert.
                    pass
        return record

    def get_frappe_records(self, last_sync_date_utc: datetime | None = None) -> list:
        """
        Frappe-Datensätze abrufen
        """
        filters = []
        if last_sync_date_utc:
            last_sync_date = last_sync_date_utc + self.frappe_tz_delta
            filters.append(f'["{self.config.frappe.modified_field}", ">=", "{last_sync_date.isoformat()}"]')
        frappe_response = self.frappe_api.get_all_data(self.config.doc_type, filters)
        records = frappe_response.get("data", [])
        for rec in records:
            self._cast_frappe_record(rec)
        return records

    def get_frappe_records_by_ids(self, ids: list[str | int]):
        filters = [f'["name", "in", {json.dumps(ids)}]']
        frappe_response = self.frappe_api.get_all_data(self.config.doc_type, filters)
        records = frappe_response.get("data", [])
        for rec in records:
            self._cast_frappe_record(rec)
        return records

    def get_frappe_key_record_dict(self, frappe_records: list[dict[str, any]]):
        frappe_dict: dict[tuple, dict[str, any]] = {}
        for rec in frappe_records:
            key = self.extract_key_from_frappe(rec)
            frappe_dict[key] = rec
        return frappe_dict

    def get_db_records(self, last_sync_date_utc: datetime | None = None):
        """
        DB-Datensätze abrufen
        """
        select_sql = f"SELECT * FROM {self.config.table_name}"
        params = []
        if last_sync_date_utc:
            last_sync_date = last_sync_date_utc + self.db_tz_delta
            params = [last_sync_date]
            select_sql = select_sql + f" WHERE {self.esc_db_col(self.config.db.modified_field)} >= ?"
            if self.config.db.fallback_modified_field:
                params.append(last_sync_date)
                select_sql = select_sql + f" OR {self.esc_db_col(self.config.db.fallback_modified_field)} >= ?"

        if self.config.query:
            select_sql = self.config.query
            if last_sync_date_utc:
                last_sync_date = last_sync_date_utc + self.db_tz_delta
                select_sql = self.config.query_with_timestamp
                params = [last_sync_date] * self.config.query_with_timestamp.count("?")

        return self._execute_select_query(select_sql, params)

    def get_db_records_by_ids(self, ids: list[str | int]):
        select_sql = f"SELECT * FROM {self.config.table_name}"
        id_selector = f"{self.esc_db_col(self.config.db.id_field)} IN ({', '.join(['?']*len(ids))})"

        if self.config.query:
            q = self.config.query.strip()
            select_sql = q[:-1] if q.endswith(";") else q

        if "WHERE" in select_sql:
            select_sql = select_sql + f" AND {id_selector}"
        else:
            select_sql = select_sql + f" WHERE {id_selector}"
        return self._execute_select_query(select_sql, ids)

    def get_db_key_record_dict(self, db_records: list[dict[str, any]]):
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
            res = self.frappe_api.insert_data(self.config.doc_type, frappe_data)
            if res:
                return res.get("data")

    def update_frappe_record(self, db_rec: dict, frappe_doc_name: str):
        """
        Aktualisiert einen vorhandenen Frappe-Datensatz mit den Werten aus dem DB-Datensatz.
        """
        frappe_rec = self.map_db_to_frappe(db_rec)
        frappe_data, frappe_keys = self.split_frappe_in_data_and_keys(frappe_rec)
        res = self.frappe_api.update_data(self.config.doc_type, frappe_doc_name, frappe_data)
        if res:
            return res.get("data")

    def update_db_record(self, frappe_rec: dict):
        """
        Aktualisiert einen vorhandenen DB-Datensatz mit den Werten aus dem Frappe-Datensatz.
        """
        frappe_rec_data, frappe_rec_keys = self.split_frappe_in_data_and_keys(frappe_rec)
        db_data = self.map_frappe_to_db(frappe_rec_data, warns=False)
        db_keys = self.map_frappe_to_db(frappe_rec_keys, warns=False)
        where_clause = " AND ".join([f"{self.esc_db_col(k)} = ?" for k in db_keys.keys()])
        set_clause = ", ".join([f"{self.esc_db_col(col)} = ?" for col in db_data.keys()])
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

            def insert_query(data: dict):
                columns = ", ".join(self.esc_db_col(k) for k in data.keys())
                placeholders = ", ".join(["?"] * len(data))
                sql = f"INSERT INTO {self.config.table_name} ({columns}) VALUES ({placeholders});"
                params = list(data.values())
                return sql, params

            if self.config.db.manual_id_sequence:
                conn = self.db_conn
                cursor = conn.cursor()
                try:
                    sql_next = (
                        f"SELECT ISNULL(MAX({self.config.db.id_field}), 0) + 1 FROM {self.config.table_name}"
                        f"{'' if self.config.db.manual_id_sequence_max is None else f' WHERE {self.config.db.id_field} < {self.config.db.manual_id_sequence_max}'}"
                        f"{'' if self.dry_run else ' WITH (TABLOCKX, HOLDLOCK)'};"
                    )
                    logging.debug(f"Anfrage an {self.config.db_name}\n{format_query(sql_next, [])}")
                    cursor.execute(sql_next)
                    next_nr = cursor.fetchone()[0]
                    if next_nr >= self.config.db.manual_id_sequence_max:
                        raise Exception(
                            f"Manuelle errechnete nächste ID ({next_nr}) übersteigt manual_id_sequence_max ({self.config.db.manual_id_sequence_max})"
                        )
                    db_data[self.config.db.id_field] = next_nr

                    sql, params = insert_query(db_data)
                    if self.dry_run:
                        logging.info(f"DRY_RUN: {self.config.db_name}\n{format_query(sql, params)}")
                    else:
                        logging.debug(f"Anfrage an {self.config.db_name}\n{format_query(sql, params)}")
                        cursor.execute(sql, params)
                        logging.info(f"Neuer DB-Datensatz mit manueller Id {next_nr} eingefügt.")
                        self.db_conn.commit()

                except Exception as e:
                    self.db_conn.rollback()
                    logging.error(f"Fehler bei manuellem Insert, rolle zurück: {e}")
                    return None

                finally:
                    cursor.close()
            else:
                sql, params = insert_query(db_data)
                self.execute_query(sql, params, f"Neuer DB-Datensatz wurde eingefügt.")

            where_clause = " AND ".join([f"{self.esc_db_col(k)} = ?" for k in db_data.keys()])
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
