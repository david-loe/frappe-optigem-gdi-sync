import logging
from typing import Dict, Literal

import requests
import json
from datetime import datetime, date
from decimal import Decimal


class FrappeAPI:
    def __init__(self, config: Dict[str, str], dry_run: bool):
        self.headers = {"Accept": "application/json"}
        self.limit_page_length = config.get("limit_page_length", 20)
        if not isinstance(self.limit_page_length, int) or self.limit_page_length < 1:
            logging.warning(
                "Frappe limit_page_length ist keine positive Ganzzahl. limit_page_length wird auf 20 gesetzt"
            )
            self.limit_page_length = 20
        self._setup_auth(config)
        self.dry_run = dry_run

    def _setup_auth(self, auth_config: Dict[str, str]):
        if "api_key" in auth_config and "api_secret" in auth_config:
            api_key = auth_config["api_key"]
            api_secret = auth_config["api_secret"]
            self.headers["Authorization"] = f"token {api_key}:{api_secret}"
        else:
            logging.warning(
                "Keine Frappe API-Token in der Konfiguration gefunden. Es wird ohne Authentifizierung versucht."
            )

    def send_data(self, method: Literal["PUT", "POST"], endpoint: str, data):
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

    def get_data(self, endpoint: str, params=None):
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            logging.debug(f"Daten erfolgreich von {endpoint} abgerufen.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Fehler beim Abrufen der Daten von {endpoint}: {e}")
            return None

    def get_all_data(self, endpoint: str, params=None):
        separator = "&" if "?" in endpoint else "?"
        limit_start = 0
        data = []
        while len(data) == limit_start:
            partial_endpoint = (
                f'{endpoint}{separator}limit={self.limit_page_length}&limit_start={limit_start}&fields=["*"]'
            )
            res = self.get_data(partial_endpoint, params)
            if res:
                more_data = res.get("data")
                if isinstance(more_data, list):
                    data = data + more_data
            limit_start = limit_start + self.limit_page_length
        return {"data": data}


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)  # or str(obj) if you prefer
        return super(CustomEncoder, self).default(obj)
