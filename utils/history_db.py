import logging
from datetime import datetime, timezone
from pathlib import Path

from peewee import (
    AutoField,
    CharField,
    DatabaseProxy,
    DateTimeField,
    ForeignKeyField,
    Model,
    SqliteDatabase,
    TextField,
)

db_proxy = DatabaseProxy()
DEFAULT_CRON_EXPR = ""  # leer = kein Plan hinterlegt


class BaseModel(Model):
    class Meta:
        database = db_proxy


class SyncState(BaseModel):
    id = AutoField()
    task_name = CharField(null=True)
    task_hash = CharField(unique=True)
    last_sync_date_utc = DateTimeField(null=True)


class TaskRun(BaseModel):
    id = AutoField()
    task_name = CharField()
    task_hash = CharField()
    last_sync_date_utc = DateTimeField(null=True)
    started_at = DateTimeField()
    finished_at = DateTimeField(null=True)
    status = CharField()


class TaskLog(BaseModel):
    id = AutoField()
    run = ForeignKeyField(TaskRun, backref="logs", on_delete="CASCADE")
    created_at = DateTimeField()
    level = CharField()
    message = TextField()


class SchedulerSettings(BaseModel):
    key = CharField(primary_key=True)
    value = TextField()


class TaskHistoryDB:
    """
    SQLite-basierte Ablage für Sync-Zustände, Runs und Log-Einträge (per peewee).
    """

    def __init__(self, db_path: str):
        path = Path(db_path)
        if path.parent:
            path.parent.mkdir(parents=True, exist_ok=True)

        self.db = SqliteDatabase(path)
        db_proxy.initialize(self.db)
        self.db.connect()
        # safe=True stellt sicher, dass wir auch bei bestehenden Tabellen
        # (z. B. nach einem Neustart) keine Fehler bekommen.
        self.db.create_tables([SyncState, TaskRun, TaskLog, SchedulerSettings], safe=True)

    def close(self):
        if not self.db.is_closed():
            self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def get_last_sync_date(self, task_hash: str) -> datetime | None:
        row = SyncState.get_or_none(SyncState.task_hash == task_hash)
        return row.last_sync_date_utc if row else None

    def save_sync_date(self, task_name: str, task_hash: str, date: datetime):
        SyncState.insert(task_name=task_name, task_hash=task_hash, last_sync_date_utc=date).on_conflict(
            conflict_target=[SyncState.task_hash],
            update={SyncState.task_name: task_name, SyncState.last_sync_date_utc: date},
        ).execute()

    def start_run(
        self, task_name: str, task_hash: str, last_sync_date_utc: datetime | None, started_at: datetime
    ) -> int:
        run = TaskRun.create(
            task_name=task_name,
            task_hash=task_hash,
            last_sync_date_utc=last_sync_date_utc,
            started_at=started_at,
            status="running",
        )
        return run.id

    def finish_run(self, run_id: int, status: str, finished_at: datetime):
        TaskRun.update(finished_at=finished_at, status=status).where(TaskRun.id == run_id).execute()

    def insert_log(self, run_id: int, level: str, message: str, created_at: datetime):
        TaskLog.create(run=run_id, created_at=created_at, level=level, message=message)

    def _get_setting(self, key: str):
        row = SchedulerSettings.get_or_none(SchedulerSettings.key == key)
        return row.value if row else None

    def _set_setting(self, key: str, value: str):
        SchedulerSettings.insert(key=key, value=value).on_conflict(
            conflict_target=[SchedulerSettings.key],
            update={SchedulerSettings.value: value},
        ).execute()

    def get_schedule(self):
        cron_expr = (self._get_setting("cron") or DEFAULT_CRON_EXPR).strip()
        return {"cron": cron_expr or None}

    def set_cron_expr(self, cron_expr: str):
        self._set_setting("cron", cron_expr.strip())

    def list_runs(self, limit: int = 50, task_name: str | None = None):
        query = TaskRun.select().order_by(TaskRun.started_at.desc()).limit(limit)
        if task_name:
            query = query.where(TaskRun.task_name == task_name)
        runs = []
        for row in query:
            runs.append(
                {
                    "id": row.id,
                    "task_name": row.task_name,
                    "task_hash": row.task_hash,
                    "last_sync_date_utc": row.last_sync_date_utc,
                    "started_at": row.started_at,
                    "finished_at": row.finished_at,
                    "status": row.status,
                }
            )
        return runs

    def get_run(self, run_id: int):
        row = TaskRun.get_or_none(TaskRun.id == run_id)
        if not row:
            return None
        return {
            "id": row.id,
            "task_name": row.task_name,
            "task_hash": row.task_hash,
            "last_sync_date_utc": row.last_sync_date_utc,
            "started_at": row.started_at,
            "finished_at": row.finished_at,
            "status": row.status,
        }

    def prune_runs(self, task_name: str, status: str, keep_last: int | None):
        """
        Remove old TaskRun entries so that at most `keep_last` remain for the given task/status.
        """
        if keep_last is None:
            return

        ids_to_delete = (
            TaskRun.select(TaskRun.id)
            .where(TaskRun.task_name == task_name, TaskRun.status == status)
            .order_by(TaskRun.started_at.desc(), TaskRun.id.desc())
            .offset(keep_last)
        )
        TaskRun.delete().where(TaskRun.id.in_(ids_to_delete)).execute()

    # Convenience for tests / inspection
    def get_logs(self, run_id: int):
        return self.get_run_logs(run_id)

    def get_run_logs(self, run_id: int, limit: int | None = None):
        rows = TaskLog.select(TaskLog.created_at, TaskLog.level, TaskLog.message).where(TaskLog.run == run_id)
        rows = rows.order_by(TaskLog.id.asc())
        if limit:
            rows = rows.limit(limit)
        return [{"created_at": r.created_at, "level": r.level, "message": r.message} for r in rows]


class SQLiteRunLogHandler(logging.Handler):
    """
    Logging-Handler, der alle Log-Einträge eines Runs in der SQLite-DB ablegt.
    """

    def __init__(self, history_db: TaskHistoryDB, run_id: int):
        super().__init__()
        self.history_db = history_db
        self.run_id = run_id

    def emit(self, record: logging.LogRecord):
        try:
            message = self.format(record)
        except Exception:
            message = record.getMessage()
        created_at = datetime.fromtimestamp(record.created, tz=timezone.utc).replace(tzinfo=None)
        try:
            self.history_db.insert_log(self.run_id, record.levelname, message, created_at)
        except Exception:
            # Logging darf hier keinen weiteren Fehler werfen.
            pass
