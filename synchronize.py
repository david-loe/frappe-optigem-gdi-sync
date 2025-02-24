import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import sys

import yaml
from api.database import DatabaseConnection
from config import Config, TaskConfig
from sync.bidirectional import BidirectionalSyncTask
from sync.db_to_frappe import DbToFrappeSyncTask
from api.frappe import FrappeAPI
from sync.frappe_to_db import FrappeToDbSyncTask
from sync.task import SyncTaskBase
from utils.yaml_database import YamlDatabase


class SyncManager:
    def __init__(self, config: Config):
        self.config = config
        if config.dry_run:
            logging.info("Sync läuft im Dry-Run Modus")
        self.db_conn = DatabaseConnection(config.databases)
        self.frappe_api = FrappeAPI(config.frappe, config.dry_run)
        self.tasks = self._load_tasks(config.tasks)
        self.timestamp_db = YamlDatabase(config.timestamp_file)

    def _load_tasks(self, task_configs: dict[str, TaskConfig]):
        tasks: list[SyncTaskBase] = []
        for task_name, task_config in task_configs.items():
            task = self._create_sync_task(task_name, task_config)
            tasks.append(task)
        return tasks

    def _create_sync_task(self, task_name: str, task_config: TaskConfig) -> SyncTaskBase:
        if task_config.direction == "db_to_frappe":
            return DbToFrappeSyncTask(task_name, task_config, self.db_conn, self.frappe_api, self.config.dry_run)
        elif task_config.direction == "frappe_to_db":
            return FrappeToDbSyncTask(task_name, task_config, self.db_conn, self.frappe_api, self.config.dry_run)
        elif task_config.direction == "bidirectional":
            return BidirectionalSyncTask(task_name, task_config, self.db_conn, self.frappe_api, self.config.dry_run)

    def run(self):
        for task in self.tasks:
            last_sync_date_utc = self.get_last_sync_date(task.config)
            log = f"Starte Sync Task '{task.name}'"
            if last_sync_date_utc:
                log = log + f" ab {last_sync_date_utc}"
            logging.info(log)
            task.sync(last_sync_date_utc)
            self.save_sync_date(
                task.name,
                task.config,
                datetime.now(timezone.utc).replace(tzinfo=None)
                + timedelta(seconds=self.config.timestamp_buffer_seconds),
            )
        self.db_conn.close_connections()

    def get_last_sync_date(self, task_config: TaskConfig) -> dict[str, datetime]:
        if not task_config.use_last_sync_date:
            return None
        hash = self._gen_task_hash(task_config)
        entries = self.timestamp_db.get("timestamps")
        if entries:
            for task_name, entry in entries.items():
                if entry["hash"] == hash:
                    return datetime.fromisoformat(entry["last_sync_date_utc"])
        return None

    def save_sync_date(self, task_name: str, task_config: TaskConfig, date: datetime):
        if self.config.dry_run:
            return
        new_entry = {"hash": self._gen_task_hash(task_config), "last_sync_date_utc": date.isoformat()}
        entries = self.timestamp_db.get("timestamps")
        if not entries:
            entries = {}

        for _task_name, entry in entries.items():
            if entry["hash"] == hash:
                entries.pop(_task_name)

        entries[task_name] = new_entry
        self.timestamp_db.insert("timestamps", entries)

    def _gen_task_hash(self, task_config: TaskConfig):
        task_dict = task_config.model_dump(exclude={"use_last_sync_date", "delete"})
        json_data = json.dumps(task_dict, sort_keys=True).encode("utf-8")
        return hashlib.sha256(json_data).hexdigest()


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
            config_file = yaml.safe_load(file)
            config_file["dry_run"] = args.dry_run or config_file.get("dry_run", False)
    except Exception as e:
        logger.error(f"Fehler beim Laden der Konfigurationsdatei {args.config}: {e}")
        sys.exit(1)

    # Konfiguration validieren
    config = Config(**config_file)

    # Dry-Run von den Argumenten setzen

    sync_manager = SyncManager(config)
    sync_manager.run()


if __name__ == "__main__":
    main()
