import argparse
import logging
import sys

import yaml
from config import Config
from sync.manager import gen_task_hash


def main():
    parser = argparse.ArgumentParser(description="Frappe ↔️ Optigem / GDI Lohn & Gehalt")
    parser.add_argument("--config", default="config.yaml", help="Pfad zur Konfigurationsdatei")

    args = parser.parse_args()

    logging.basicConfig()
    logger = logging.getLogger(__name__)

    # YAML-Konfigurationsdatei laden
    try:
        with open(args.config, "r", encoding="utf-8") as file:
            config_file = yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Fehler beim Laden der Konfigurationsdatei {args.config}: {e}")
        sys.exit(1)

    # Konfiguration validieren
    config = Config(**config_file)

    for task_name, task_config in config.tasks.items():
        print(task_name, gen_task_hash(task_config))


if __name__ == "__main__":
    main()
