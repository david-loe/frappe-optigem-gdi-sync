from abc import ABC, abstractmethod
from typing import Any, Literal
from database import DatabaseConnection
from frappe import FrappeAPI


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
        self.direction: Literal["db_to_frappe", "db_to_frappe"] = self.task_config.get("direction", "db_to_frappe")
        self.key_fields: str | list[str] = self.task_config.get("key_fields")
        self.process_all: bool = self.task_config.get("process_all", False)
        self.create_new: bool = self.task_config.get("create_new", False)
        self.query: str = self.task_config.get("query")
        self.table_name: str = self.task_config.get("table_name")

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
