import argparse
import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from croniter import croniter
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
import uvicorn

from sync.manager import SyncManager, resolve_timestamp_path
from utils.config_loader import load_config_file
from utils.history_db import TaskHistoryDB


class SyncService:
    """
    Verwaltet den periodischen Sync und stellt Methoden bereit,
    um ihn manuell zu starten oder den Cron-Plan anzupassen.
    """

    def __init__(self, config_path: str, dry_run: bool = False, initial_cron: str | None = None):
        self.config_path = config_path
        self.dry_run = dry_run
        self._run_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._scheduler_thread: threading.Thread | None = None

        self._reload_config()

        self.cron_expr: str | None = None
        self._load_schedule_from_db()
        if initial_cron:
            try:
                self.set_cron(initial_cron)
            except ValueError:
                logging.warning("Initialer Cron aus ENV/CLI ist ungültig und wird ignoriert: '%s'", initial_cron)

    def _load_schedule_from_db(self):
        with TaskHistoryDB(self.history_db_path) as history_db:
            schedule = history_db.get_schedule()
        self._apply_schedule(schedule)

    def _reload_config(self):
        self.config = load_config_file(self.config_path, self.dry_run)
        self.history_db_path = resolve_timestamp_path(self.config_path, self.config.timestamp_file)

    def _apply_schedule(self, schedule: dict):
        expr = (schedule.get("cron") or "").strip()
        if not expr:
            self.cron_expr = None
            return
        if croniter.is_valid(expr):
            self.cron_expr = expr
        else:
            logging.warning("Ungültiger Cron-Ausdruck '%s', Scheduler pausiert bis neuer Wert gesetzt wird", expr)
            self.cron_expr = None

    def _next_wait_seconds(self) -> float:
        if not self.cron_expr:
            return None
        now = datetime.now()
        try:
            next_run = croniter(self.cron_expr, now).get_next(datetime)
            wait_seconds = (next_run - now).total_seconds()
            return max(wait_seconds, 1)
        except Exception:
            logging.exception("Cron-Berechnung fehlgeschlagen, Scheduler pausiert bis neuer Wert gesetzt wird")
            self.cron_expr = None
            return None

    def _schedule_label(self) -> str:
        return f"Cron '{self.cron_expr}'" if self.cron_expr else "kein Cron gesetzt"

    def start(self):
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            return
        self._stop_event.clear()
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()
        logging.info("Scheduler gestartet: %s", self._schedule_label())

    def stop(self):
        self._stop_event.set()
        self._wake_event.set()
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=2)

    def set_cron(self, cron_expr: str):
        expr = cron_expr.strip()
        if not croniter.is_valid(expr):
            raise ValueError("Ungültiger Cron-Ausdruck")
        self.cron_expr = expr
        with TaskHistoryDB(self.history_db_path) as history_db:
            history_db.set_cron_expr(expr)
        self._wake_event.set()
        logging.info("Sync-Cron aktualisiert auf '%s'", expr)

    def _scheduler_loop(self):
        while not self._stop_event.is_set():
            wait_seconds = self._next_wait_seconds()
            if wait_seconds is None:
                # Kein gültiger Plan: warten, bis ein neuer Cron gesetzt oder Stopp ausgelöst wird
                self._wake_event.wait()
                self._wake_event.clear()
                continue

            interrupted = self._wake_event.wait(timeout=wait_seconds)
            self._wake_event.clear()

            if self._stop_event.is_set():
                break

            if interrupted:
                # Plan wurde geändert, neu warten
                continue

            if not self.trigger_sync(reason="cron"):
                logging.info("Überspringe geplanten Lauf, da bereits ein Sync läuft.")

    def trigger_sync(self, reason: str = "manual", task_names: list[str] | None = None) -> bool:
        if not self._run_lock.acquire(blocking=False):
            return False

        threading.Thread(target=self._run_sync, args=(reason, task_names), daemon=True).start()
        return True

    def _run_sync(self, reason: str, task_names: list[str] | None = None):
        try:
            selection = f" (Tasks: {', '.join(task_names)})" if task_names else ""
            logging.info("Starte Sync (%s)%s", reason, selection)
            self._reload_config()
            manager = SyncManager(self.config, self.config_path)
            manager.run(task_names=task_names)
            # Plan evtl. neu laden (falls z. B. DB erneuert wurde)
            self._load_schedule_from_db()
        except Exception:
            logging.exception("Sync fehlgeschlagen (%s)", reason)
        finally:
            self._run_lock.release()

    @property
    def is_running(self) -> bool:
        return self._run_lock.locked()

    def list_config_tasks(self) -> list[dict[str, str]]:
        self._reload_config()
        return [{"name": name, "direction": task.direction} for name, task in self.config.tasks.items()]

    def normalize_task_names(self, task_names: list[str] | None) -> list[str] | None:
        self._reload_config()
        if task_names is None:
            return None
        cleaned = [name.strip() for name in task_names if name and name.strip()]
        if not cleaned:
            raise ValueError("Es wurde kein Task-Name angegeben.")

        available = set(self.config.tasks.keys())
        unknown = [name for name in cleaned if name not in available]
        if unknown:
            raise ValueError(f"Unbekannte Tasks: {', '.join(sorted(unknown))}")

        seen = set()
        unique_tasks = []
        for name in cleaned:
            if name not in seen:
                unique_tasks.append(name)
                seen.add(name)
        return unique_tasks


class ScheduleRequest(BaseModel):
    cron: Optional[str] = None


class RunRequest(BaseModel):
    tasks: Optional[list[str]] = None


def create_app(service: SyncService) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service.start()
        try:
            yield
        finally:
            service.stop()

    app = FastAPI(title="Frappe ↔ Optigem Sync Service", version="0.1.0", lifespan=lifespan)
    app.state.service = service
    frontend_index = Path(__file__).parent / "frontend" / "index.html"

    @app.get("/", include_in_schema=False)
    async def serve_frontend():
        if not frontend_index.exists():
            raise HTTPException(status_code=404, detail="Frontend nicht gefunden")
        return FileResponse(frontend_index)

    @app.get("/health")
    async def health():
        return {
            "status": "ok",
            "running": service.is_running,
            "cron": service.cron_expr,
        }

    @app.get("/schedule")
    async def get_schedule():
        return {
            "cron": service.cron_expr,
        }

    @app.get("/tasks")
    async def list_tasks():
        return {"tasks": service.list_config_tasks()}

    @app.put("/schedule")
    async def update_schedule(body: ScheduleRequest):
        if body.cron is None:
            raise HTTPException(status_code=400, detail="cron muss gesetzt sein")
        try:
            service.set_cron(body.cron)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        return {
            "cron": service.cron_expr,
        }

    @app.post("/run")
    async def run_now(body: RunRequest | None = None):
        task_names = body.tasks if body else None
        try:
            normalized = service.normalize_task_names(task_names)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        if not service.trigger_sync(reason="manual", task_names=normalized):
            raise HTTPException(status_code=409, detail="Sync läuft bereits")
        task_list = normalized or list(service.config.tasks.keys())
        return {"status": "started", "tasks": task_list}

    @app.get("/runs")
    async def list_runs(limit: int = 50, task_name: Optional[str] = None):
        if limit < 1:
            raise HTTPException(status_code=400, detail="limit muss mindestens 1 sein")
        limit = min(limit, 500)
        with TaskHistoryDB(service.history_db_path) as history_db:
            runs = history_db.list_runs(limit=limit, task_name=task_name)
        return {"runs": runs}

    @app.get("/runs/{run_id}")
    async def get_run(run_id: int):
        with TaskHistoryDB(service.history_db_path) as history_db:
            run = history_db.get_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run nicht gefunden")
        return run

    @app.get("/runs/{run_id}/logs")
    async def get_run_logs(run_id: int, limit: int = 200):
        if limit < 1:
            raise HTTPException(status_code=400, detail="limit muss mindestens 1 sein")
        limit = min(limit, 1000)
        with TaskHistoryDB(service.history_db_path) as history_db:
            run = history_db.get_run(run_id)
            if not run:
                raise HTTPException(status_code=404, detail="Run nicht gefunden")
            logs = history_db.get_run_logs(run_id, limit=limit)
        return {"run": run, "logs": logs}

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Starte den Sync Web Service")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--loglevel", default="INFO")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cron", help="Cron-Ausdruck für den Scheduler (überschreibt gespeicherten Wert)")
    args = parser.parse_args()

    log_level = getattr(logging, args.loglevel.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format="%(asctime)s [%(levelname)s] %(message)s")

    service = SyncService(args.config, dry_run=args.dry_run, initial_cron=args.cron)
    app = create_app(service)

    uvicorn.run(app, host=args.host, port=args.port, log_level=args.loglevel.lower())
