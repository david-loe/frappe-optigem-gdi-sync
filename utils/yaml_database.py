import yaml


class YamlDatabase:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def _load_data(self) -> dict[str, any]:
        """Lädt Daten aus der YAML-Datei."""
        try:
            with open(self.file_path, "r") as file:
                return yaml.safe_load(file) or {}  # Lädt Daten oder gibt ein leeres Dict zurück
        except FileNotFoundError:
            return {}  # Datei existiert noch nicht, gib leeres Dict zurück

    def _save_data(self, data: dict[str, any]) -> None:
        """Speichert Daten in der YAML-Datei."""
        with open(self.file_path, "w") as file:
            yaml.safe_dump(data, file)

    def insert(self, key: str, value: any) -> None:
        """Fügt ein neues Element hinzu oder aktualisiert ein bestehendes."""
        data = self._load_data()
        data[key] = value
        self._save_data(data)

    def get(self, key: str) -> any:
        """Ruft ein Element anhand des Schlüssels ab."""
        data = self._load_data()
        return data.get(key, None)

    def delete(self, key: str) -> None:
        """Löscht ein Element anhand des Schlüssels."""
        data = self._load_data()
        if key in data:
            del data[key]
            self._save_data(data)
