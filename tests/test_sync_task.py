import logging
from datetime import datetime, timedelta

from config import DbToFrappeTaskConfig, TaskDbBase, TaskFrappeBase
from sync.bidirectional import compare_datetimes
from sync.manager import SyncManager, gen_task_hash
from sync.task import SyncTaskBase
from utils.history_db import SQLiteRunLogHandler, TaskHistoryDB, SyncState


class DummyTask(SyncTaskBase[DbToFrappeTaskConfig]):
    def sync(self, last_sync_date_utc=None):
        pass


def make_task(config: DbToFrappeTaskConfig, frappe_delta=timedelta(), db_delta=timedelta()):
    task = DummyTask.__new__(DummyTask)
    task.name = "dummy"
    task.config = config
    task.frappe_api = type("Frappe", (), {"tz_delta": frappe_delta})
    task.dry_run = False
    task.db_conn = None
    task.esc_db_col = lambda x: x
    task.frappe_tz_delta = frappe_delta
    task.db_tz_delta = db_delta
    return task


def make_config(
    mapping: dict,
    *,
    value_mapping: dict | None = None,
    use_strict_value_mapping: bool = False,
    frappe_int_fields: list[str] | None = None,
):
    return DbToFrappeTaskConfig(
        direction="db_to_frappe",
        doc_type="DocType",
        db_name="db",
        mapping=mapping,
        key_fields=["modified"],
        table_name="table",
        frappe=TaskFrappeBase(modified_fields=["modified"], int_fields=frappe_int_fields or []),
        db=TaskDbBase(modified_fields=["updated_at"]),
        value_mapping=value_mapping or {},
        use_strict_value_mapping=use_strict_value_mapping,
    )


def test_timezone_harmonization_frappe_to_db():
    config = make_config({"modified": "updated_at"})
    task = make_task(config, frappe_delta=timedelta(hours=2), db_delta=timedelta(hours=1))
    modified = datetime(2024, 1, 1, 12, 0)

    result = task.map_frappe_to_db({"modified": modified})

    assert result["updated_at"] == datetime(2024, 1, 1, 11, 0)


def test_value_mapping_strict_skips_unknown_values():
    config = make_config(
        {"modified": "updated_at", "status": "status_db"},
        value_mapping={"status": {"open": 1}},
        use_strict_value_mapping=True,
    )
    task = make_task(config)
    record = {"modified": datetime(2024, 1, 1), "status": "closed"}

    result = task.map_frappe_to_db(record)

    assert "status_db" not in result


def test_value_mapping_reverse_db_to_frappe():
    config = make_config(
        {"modified": "updated_at", "status": "status_db"},
        value_mapping={"status": {"open": 1}},
    )
    task = make_task(config)
    record = {"updated_at": datetime(2024, 1, 1), "status_db": 1}

    result = task.map_db_to_frappe(record)

    assert result["status"] == "open"


def test_save_sync_date_replaces_entries_with_same_hash(tmp_path):
    config = make_config({"modified": "updated_at"})
    timestamp_file = tmp_path / "data.db"
    manager = SyncManager.__new__(SyncManager)
    manager.config = type("Cfg", (), {"dry_run": False, "timestamp_file": "data.db"})
    manager.history_db = TaskHistoryDB(str(timestamp_file))

    existing_hash = gen_task_hash(config)
    manager.history_db.save_sync_date("old_task", existing_hash, datetime(2020, 1, 1))

    manager.save_sync_date("new_task", config, datetime(2024, 1, 1))
    rows = list(SyncState.select())

    assert len(rows) == 1
    assert rows[0].task_name == "new_task"
    assert rows[0].task_hash == existing_hash
    assert rows[0].last_sync_date_utc == datetime(2024, 1, 1)
    manager.history_db.close()


def test_timezone_harmonization_db_to_frappe():
    config = make_config({"modified": "updated_at"})
    task = make_task(config, frappe_delta=timedelta(hours=2), db_delta=timedelta(hours=1))
    value = datetime(2024, 2, 2, 12, 0)

    result = task.map_db_to_frappe({"updated_at": value})

    assert result["modified"] == datetime(2024, 2, 2, 13, 0)


def test_value_mapping_strict_db_to_frappe_skips_unknown_values():
    config = make_config(
        {"modified": "updated_at", "status": "status_db"},
        value_mapping={"status": {"open": 1}},
        use_strict_value_mapping=True,
    )
    task = make_task(config)
    record = {"updated_at": datetime(2024, 1, 1), "status_db": 2}

    result = task.map_db_to_frappe(record)

    assert "status" not in result


def test_cast_frappe_record_parses_datetime_and_int():
    config = make_config({"modified": "updated_at", "count": "cnt_db"}, frappe_int_fields=["count"])
    task = make_task(config)
    record = {"modified": "2024-01-01T10:00:00", "count": "5", "other": "keep"}

    parsed = task._cast_frappe_record(record)

    assert parsed["modified"] == datetime(2024, 1, 1, 10, 0)
    assert parsed["count"] == 5
    assert parsed["other"] == "keep"


def test_compare_datetimes_honors_tolerance():
    dt1 = datetime(2024, 3, 3, 12, 0, 0, 50_000)  # +50ms
    dt2 = datetime(2024, 3, 3, 12, 0, 0, 0)

    assert compare_datetimes(dt1, dt2, 100) == 0
    assert compare_datetimes(dt1, dt2, 10) == 1


def test_sqlite_log_handler_writes_logs(tmp_path):
    history = TaskHistoryDB(str(tmp_path / "data.db"))
    started_at = datetime(2024, 1, 1, 12, 0, 0)
    run_id = history.start_run("task", "hash", None, started_at)
    handler = SQLiteRunLogHandler(history, run_id)
    handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))

    logger = logging.getLogger(f"test_logger_{run_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(handler)

    logger.info("hello")
    logger.warning("warn")

    logger.removeHandler(handler)
    handler.close()
    history.finish_run(run_id, "success", datetime(2024, 1, 1, 12, 5, 0))

    logs = history.get_logs(run_id)

    assert logs[0]["message"].endswith("hello")
    assert logs[0]["level"] == "INFO"
    assert logs[1]["message"].endswith("warn")
    assert logs[1]["level"] == "WARNING"
    history.close()


def test_cron_get_and_set(tmp_path):
    history = TaskHistoryDB(str(tmp_path / "data.db"))

    schedule = history.get_schedule()
    assert schedule["cron"] is None

    history.set_cron_expr("0 5 * * *")
    schedule = history.get_schedule()
    assert schedule["cron"] == "0 5 * * *"
    history.close()


def test_list_runs_orders_by_started_at_desc(tmp_path):
    history = TaskHistoryDB(str(tmp_path / "data.db"))
    first = history.start_run("task", "hash", None, datetime(2024, 1, 1, 12, 0, 0))
    history.finish_run(first, "success", datetime(2024, 1, 1, 12, 1, 0))
    second = history.start_run("task", "hash", None, datetime(2024, 1, 1, 13, 0, 0))
    history.finish_run(second, "error", datetime(2024, 1, 1, 13, 2, 0))

    runs = history.list_runs()

    assert runs[0]["id"] == second
    assert runs[0]["status"] == "error"
    assert runs[1]["id"] == first
    history.close()
