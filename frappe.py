import logging
from typing import Dict

import requests
import json
from datetime import datetime, date
from decimal import Decimal


class FrappeAPI:
    def __init__(self, auth_config: Dict[str, str], dry_run: bool):
        self.headers = {"Accept": "application/json"}
        self._setup_auth(auth_config)
        self.dry_run = dry_run

    def _setup_auth(self, auth_config):
        if "api_key" in auth_config and "api_secret" in auth_config:
            api_key = auth_config["api_key"]
            api_secret = auth_config["api_secret"]
            self.headers["Authorization"] = f"token {api_key}:{api_secret}"
        else:
            logging.warning(
                "Keine Frappe API-Token in der Konfiguration gefunden. Es wird ohne Authentifizierung versucht."
            )

    def send_data(self, method, endpoint, data):
        try:
            json_data = json.dumps(data, cls=CustomEncoder)
            if self.dry_run:
                logging.info(
                    f"""DRY_RUN: {method} {endpoint}
                        {json_data}"""
                )
                return None
            headers = self.headers.copy()
            headers["Content-Type"] = "application/json"
            if method == "POST":
                response = requests.post(endpoint, data=json_data, headers=self.headers)
            elif method == "PUT":
                response = requests.put(endpoint, data=json_data, headers=self.headers)
            else:
                logging.error(f"Unbekannte HTTP-Methode: {method}")
                return None
            response.raise_for_status()
            logging.info(f"Daten erfolgreich an {method} {endpoint} gesendet.")
            logging.debug(f"{json_data}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Fehler beim Senden der Daten an {method} {endpoint}: {e}")
            logging.error(f"{json_data}")
            return None

    def get_data(self, endpoint, params=None):
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            logging.debug(f"Daten erfolgreich von {endpoint} abgerufen.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Fehler beim Abrufen der Daten von {endpoint}: {e}")
            return None


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)  # or str(obj) if you prefer
        return super(CustomEncoder, self).default(obj)
