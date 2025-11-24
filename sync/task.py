from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import json
import logging
from typing import Generic, Literal, TypeVar

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
        self.frappe_tz_delta = frappe_api.tz_delta or timedelta()
        self.db_tz_delta = get_time_zone(self.db_conn) or timedelta()

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

    def _execute_select_query(self, sql: str, params: list | None = None):
        params = params or []
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

    def _should_adjust_timezone(self, frappe_field: str, db_field: str) -> bool:
        if not self.config.frappe or not self.config.db:
            return False
        return frappe_field in self.config.frappe.modified_fields and db_field in self.config.db.modified_fields

    def _adjust_timezone(
        self, value: any, *, frappe_field: str, db_field: str, direction: Literal["frappe_to_db", "db_to_frappe"]
    ):
        if not isinstance(value, datetime):
            return value
        if not self._should_adjust_timezone(frappe_field, db_field):
            return value
        if direction == "frappe_to_db":
            return value - self.frappe_tz_delta + self.db_tz_delta
        return value - self.db_tz_delta + self.frappe_tz_delta

    def _apply_value_mapping(
        self, value: any, frappe_field: str, *, direction: Literal["frappe_to_db", "db_to_frappe"], warns: bool
    ) -> tuple[any, bool]:
        mapping = self.config.value_mapping.get(frappe_field)
        if not mapping:
            return value, True

        # use value_mapping
        if direction == "frappe_to_db":
            if value in mapping:
                return mapping[value], True
            if self.config.use_strict_value_mapping and warns:
                logging.warning(
                    f"Kein Wert in value_mapping gefunden für frappe-Feld '{frappe_field}' und Wert '{value}'."
                )
            return value, not self.config.use_strict_value_mapping

        for f_value, db_value in mapping.items():
            if db_value == value:
                return f_value, True
        if self.config.use_strict_value_mapping and warns:
            logging.warning(
                f"Kein Wert in value_mapping gefunden für DB-Feld "
                f"'{self.config.mapping.get(frappe_field, 'unbekannt')}' und Wert '{value}'."
            )
        return value, not self.config.use_strict_value_mapping

    def map_frappe_to_db(self, record: dict, warns=True) -> dict:
        """
        Übersetzt einen Frappe-Datensatz in ein DB-Datenformat anhand des Mapping.
        """
        db_data = {}
        for frappe_field, db_column in self.config.mapping.items():
            if frappe_field not in record:
                if warns:
                    logging.warning(f"Feld '{frappe_field}' fehlt im Frappe-Datensatz {record}.")
                continue

            value = record[frappe_field]
            if value is None:
                continue

            value = self._adjust_timezone(
                value, frappe_field=frappe_field, db_field=db_column, direction="frappe_to_db"
            )
            value, valid = self._apply_value_mapping(value, frappe_field, direction="frappe_to_db", warns=warns)
            if valid:
                db_data[db_column] = value
        return db_data

    def map_db_to_frappe(self, record: dict, warns=True) -> dict:
        """
        Übersetzt einen DB-Datensatz in ein Frappe-Datenformat anhand des inversen Mapping.
        """
        frappe_data = {}
        for frappe_field, db_column in self.config.mapping.items():
            if db_column not in record:
                if warns:
                    logging.warning(f"Spalte '{db_column}' fehlt im DB-Datensatz {record}.")
                continue

            value = record[db_column]
            if value is None:
                continue

            value = self._adjust_timezone(
                value, frappe_field=frappe_field, db_field=db_column, direction="db_to_frappe"
            )
            value, valid = self._apply_value_mapping(value, frappe_field, direction="db_to_frappe", warns=warns)
            if valid:
                frappe_data[frappe_field] = value
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
        if not self.config.frappe:
            return record
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
            if not self.config.frappe:
                raise ValueError("Frappe-Konfiguration fehlt, um Datensätze anhand des Änderungsdatums zu filtern.")
            last_sync_date = last_sync_date_utc + self.frappe_tz_delta
            for modified_field in self.config.frappe.modified_fields:
                filters.append(f'["{modified_field}", ">=", "{last_sync_date.isoformat()}"]')
        frappe_response = self.frappe_api.get_all_data(self.config.doc_type, filters, or_filters=True)
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
            if not self.config.db:
                raise ValueError("DB-Konfiguration fehlt, um Datensätze anhand des Änderungsdatums zu filtern.")
            last_sync_date = last_sync_date_utc + self.db_tz_delta
            is_first_condition = True
            for modified_field in self.config.db.modified_fields:
                conjunction = "WHERE" if is_first_condition else " OR"
                select_sql = select_sql + f"{conjunction} {self.esc_db_col(modified_field)} >= ?"
                params.append(last_sync_date)
                is_first_condition = False

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
            frappe_rec_data, frappe_rec_keys = self.split_frappe_in_data_and_keys(frappe_rec)
            db_only_keys = self.map_frappe_to_db(frappe_rec_keys, warns=False)
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
                        f"{'' if self.dry_run else ' WITH (TABLOCKX, HOLDLOCK)'}"
                        f"{';' if self.config.db.manual_id_sequence_max is None else f' WHERE {self.config.db.id_field} < {self.config.db.manual_id_sequence_max};'}"
                    )
                    logging.debug(f"Anfrage an {self.config.db_name}\n{format_query(sql_next, [])}")
                    cursor.execute(sql_next)
                    next_nr = cursor.fetchone()[0]
                    if (
                        self.config.db.manual_id_sequence_max is not None
                        and next_nr >= self.config.db.manual_id_sequence_max
                    ):
                        raise Exception(
                            f"Manuelle errechnete nächste ID ({next_nr}) übersteigt manual_id_sequence_max ({self.config.db.manual_id_sequence_max})"
                        )
                    db_data[self.config.db.id_field] = next_nr
                    db_only_keys[self.config.db.id_field] = next_nr

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

            where_clause = " AND ".join([f"{self.esc_db_col(k)} = ?" for k in db_only_keys.keys()])
            sql_select = f"SELECT * FROM {self.config.table_name} WHERE {where_clause}"
            results = self._execute_select_query(sql_select, list(db_only_keys.values()))
            if len(results) == 0:
                logging.warning(f"DB-Datensatz konnte nach UPDATE nicht gefunden werden: {db_only_keys}")
                return None
            elif len(results) == 1:
                return results[0]
            else:
                logging.warning(f"Nach UPDATE konnten mehrere DB-Datensätze gefunden werden: {db_only_keys}")
                return results[0]
