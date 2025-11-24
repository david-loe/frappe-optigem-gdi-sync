import argparse
import logging
import sys

from sync.manager import SyncManager
from utils.config_loader import load_config_file


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
        config = load_config_file(args.config, args.dry_run)
    except Exception as e:
        logger.error(f"Fehler beim Laden der Konfigurationsdatei {args.config}: {e}")
        sys.exit(1)

    sync_manager = SyncManager(config, args.config)
    sync_manager.run()


if __name__ == "__main__":
    main()
