import argparse
import logging
import sys
from typing import Any, Dict

import yaml
from database import DatabaseConnection
from db_to_frappe_sync import DbToFrappeSyncTask
from frappe import FrappeAPI
from frappe_to_db_sync import FrappeToDbSyncTask
from sync_task import SyncTaskBase


class SyncManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.dry_run = config.get("dry_run")
        if self.dry_run:
            logging.info("Sync läuft im Dry-Run Modus")
        self.db_conn = DatabaseConnection(config)
        self.frappe_api = FrappeAPI(config.get("frappe_auth", {}), self.dry_run)
        self.tasks = self._load_tasks()

    def _load_tasks(self):
        tasks: list[SyncTaskBase] = []
        for task_config in self.config.get("tasks", []):
            task = self._create_sync_task(task_config)
            tasks.append(task)
        return tasks

    def _create_sync_task(self, task_config: dict[str, Any]) -> SyncTaskBase:
        """Erzeugt basierend auf der Konfiguration die passende Sync-Task-Instanz."""
        direction = task_config.get("direction", "db_to_frappe")
        if direction == "db_to_frappe":
            return DbToFrappeSyncTask(task_config, self.db_conn, self.frappe_api, self.dry_run)
        elif direction == "frappe_to_db":
            return FrappeToDbSyncTask(task_config, self.db_conn, self.frappe_api, self.dry_run)
        else:
            raise ValueError(f"Unbekannte Synchronisationsrichtung: {direction}")

    def run(self):
        for task in self.tasks:
            task.sync()
        self.db_conn.close_connections()


def main():
    parser = argparse.ArgumentParser(description="Frappe ↔️ Optigem / GDI Lohn & Gehalt")
    parser.add_argument("--loglevel", default="INFO", help="Setzt das Loglevel (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    parser.add_argument("--config", default="config.yaml", help="Pfad zur Konfigurationsdatei")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Führt den Sync im Dry-Run-Modus aus (keine Änderungen werden vorgenommen)",
    )
    args = parser.parse_args()

    args = parser.parse_args()

    # Loglevel einstellen
    log_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(log_level, int):
        print(f"Ungültiges Loglevel: {args.loglevel}")
        sys.exit(1)

    logging.basicConfig(level=log_level)
    logger = logging.getLogger(__name__)

    # YAML-Konfigurationsdatei laden
    try:
        with open(args.config, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Fehler beim Laden der Konfigurationsdatei {args.config}: {e}")
        sys.exit(1)

    # Dry-Run von den Argumenten setzen
    config["dry_run"] = args.dry_run or config.get("dry_run", False)
    sync_manager = SyncManager(config)
    sync_manager.run()


if __name__ == "__main__":
    main()
