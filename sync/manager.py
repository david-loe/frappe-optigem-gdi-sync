from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os

from api.database import DatabaseConnection
from config import Config, TaskConfig
from sync.bidirectional import BidirectionalSyncTask
from sync.db_to_frappe import DbToFrappeSyncTask
from api.frappe import FrappeAPI
from sync.frappe_to_db import FrappeToDbSyncTask
from sync.task import SyncTaskBase
from utils.history_db import SQLiteRunLogHandler, TaskHistoryDB


def resolve_timestamp_path(config_path: str, timestamp_file: str) -> str:
    config_dir = os.path.dirname(config_path)
    return os.path.join(config_dir, timestamp_file)


class SyncManager:
    def __init__(self, config: Config, config_path: str, history_db: TaskHistoryDB | None = None):
        self.config = config
        if config.dry_run:
            logging.info("Sync läuft im Dry-Run Modus")
        self.db_conn = DatabaseConnection(config.databases)
        self.frappe_api = FrappeAPI(config.frappe, config.dry_run)
        self.tasks = self._load_tasks(config.tasks)
        timestamp_path = resolve_timestamp_path(config_path, config.timestamp_file)
        self.history_db = history_db or TaskHistoryDB(timestamp_path)
        self._close_history_db = history_db is None

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

    def run(self, task_names: list[str] | None = None):
        tasks_to_run = self.tasks
        if task_names is not None:
            requested = set(task_names)
            tasks_to_run = [task for task in self.tasks if task.name in requested]
            missing = [name for name in task_names if name not in {task.name for task in tasks_to_run}]
            if missing:
                logging.warning("Tasks nicht gefunden und werden übersprungen: %s", ", ".join(missing))

        if not tasks_to_run:
            logging.info("Keine Tasks im Filter gefunden, Sync wird übersprungen.")
            return

        logging.info("Führe %s Task(s) aus: %s", len(tasks_to_run), ", ".join(task.name for task in tasks_to_run))

        try:
            for task in tasks_to_run:
                last_sync_date_utc = self.get_last_sync_date(task.config)
                started_at = datetime.now(timezone.utc).replace(tzinfo=None)
                run_id = self.history_db.start_run(
                    task.name, gen_task_hash(task.config), last_sync_date_utc, started_at
                )
                handler = SQLiteRunLogHandler(self.history_db, run_id)
                root_logger = logging.getLogger()
                root_logger.addHandler(handler)
                run_status: str | None = None
                try:
                    log = f"Starte Sync Task '{task.name}'"
                    if last_sync_date_utc:
                        log = log + f" ab {last_sync_date_utc}"
                    logging.info(log)

                    task.sync(last_sync_date_utc)
                    sync_date = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(
                        seconds=self.config.timestamp_buffer_seconds
                    )
                    self.save_sync_date(task.name, task.config, sync_date)
                    self.history_db.finish_run(run_id, "success", datetime.now(timezone.utc).replace(tzinfo=None))
                    run_status = "success"
                except Exception:
                    self.history_db.finish_run(run_id, "error", datetime.now(timezone.utc).replace(tzinfo=None))
                    run_status = "error"
                    raise
                finally:
                    if run_status:
                        self._prune_task_runs(task.name, run_status)
                    root_logger.removeHandler(handler)
                    handler.close()
        finally:
            self.db_conn.close_connections()
            if self._close_history_db:
                self.history_db.close()

    def get_last_sync_date(self, task_config: TaskConfig) -> datetime | None:
        if not task_config.use_last_sync_date:
            return None
        task_hash = gen_task_hash(task_config)
        return self.history_db.get_last_sync_date(task_hash)

    def save_sync_date(self, task_name: str, task_config: TaskConfig, date: datetime):
        if self.config.dry_run:
            return
        task_hash = gen_task_hash(task_config)
        self.history_db.save_sync_date(task_name, task_hash, date)

    def _prune_task_runs(self, task_name: str, status: str):
        if status not in {"success", "error"}:
            return
        keep_last = (
            self.config.max_success_runs_per_task
            if status == "success"
            else self.config.max_error_runs_per_task
        )
        self.history_db.prune_runs(task_name, status, keep_last)


def gen_task_hash(task_config: TaskConfig):
    task_dict = task_config.model_dump(exclude={"use_last_sync_date", "delete"})
    json_data = json.dumps(task_dict, sort_keys=True).encode("utf-8")
    return hashlib.sha256(json_data).hexdigest()
