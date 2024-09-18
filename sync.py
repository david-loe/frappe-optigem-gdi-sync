import logging
import sys

import yaml
from database import DatabaseConnection
from frappe import FrappeAPI
from sync_task import SyncTask


class SyncManager:
    def __init__(self, config):
        self.config = config
        self.db_conn = DatabaseConnection(config)
        self.frappe_api = FrappeAPI(config.get('frappe_auth', {}))
        self.tasks = self._load_tasks()

    def _load_tasks(self):
        tasks = []
        for task_config in self.config.get('tasks', []):
            task = SyncTask(task_config, self.db_conn, self.frappe_api)
            tasks.append(task)
        return tasks

    def run(self):
        for task in self.tasks:
            task.execute()
        self.db_conn.close_connections()

def main():
    # Logging konfigurieren
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # YAML-Konfigurationsdatei laden
    try:
        with open('config.yaml', 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Fehler beim Laden der Konfigurationsdatei: {e}")
        sys.exit(1)

    sync_manager = SyncManager(config)
    sync_manager.run()

if __name__ == '__main__':
    main()