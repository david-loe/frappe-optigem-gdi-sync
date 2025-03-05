import argparse
import logging
import sys

import yaml
from config import Config
from sync.manager import SyncManager


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
