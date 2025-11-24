import yaml
from config import Config


def load_config_file(config_path: str, dry_run: bool = False) -> Config:
    with open(config_path, "r", encoding="utf-8") as file:
        config_file = yaml.safe_load(file) or {}
        config_file["dry_run"] = dry_run or config_file.get("dry_run", False)
    return Config(**config_file)
