import logging
from datetime import datetime
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
        self.db.create_tables([SyncState, TaskRun, TaskLog])

    def close(self):
        if not self.db.is_closed():
            self.db.close()

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

    # Convenience for tests / inspection
    def get_logs(self, run_id: int):
        rows = (
            TaskLog.select(TaskLog.created_at, TaskLog.level, TaskLog.message)
            .where(TaskLog.run == run_id)
            .order_by(TaskLog.id.asc())
        )
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
        created_at = datetime.utcfromtimestamp(record.created)
        try:
            self.history_db.insert_log(self.run_id, record.levelname, message, created_at)
        except Exception:
            # Logging darf hier keinen weiteren Fehler werfen.
            pass
