import logging
from typing import Literal

import requests
import json
from datetime import datetime, date
from decimal import Decimal

from config import FrappeAuthConfig, FrappeConfig


class FrappeAPI:
    def __init__(self, config: FrappeConfig, dry_run: bool):
        self.config = config
        self.headers = {"Accept": "application/json"}
        self._setup_auth(config)
        self.dry_run = dry_run

    def _setup_auth(self, auth_config: FrappeAuthConfig):
        api_key = auth_config.api_key
        api_secret = auth_config.api_secret
        self.headers["Authorization"] = f"token {api_key}:{api_secret}"

    def _send_data(self, method: Literal["PUT", "POST"], endpoint: str, data):
        try:
            json_data = json.dumps(data, cls=CustomEncoder)
            if self.dry_run:
                logging.info(
                    f"""DRY_RUN: {method} {endpoint}
                        {json_data}"""
                )
                return {"data": {}}
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

    def insert_data(self, doc_type: str, data):
        return self._send_data("POST", self.get_endpoint(doc_type), data)

    def update_data(self, doc_type: str, doc_name: str, data):
        return self._send_data("PUT", self.get_endpoint(doc_type, doc_name), data)

    def get_data(self, doc_type: str, doc_name: str | None = None, filters: list[str] = [], params: dict | None = None):
        endpoint = self.get_endpoint(doc_type, doc_name)
        params = params.copy() if params else {}
        if len(filters) > 0:
            params["filters"] = f"[{','.join(filters)}]"
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            logging.debug(f"Daten erfolgreich von {endpoint} ({params}) abgerufen.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Fehler beim Abrufen der Daten von {endpoint} ({params}): {e}")
            return None

    def get_all_data(self, doc_type: str, filters: list[str] = [], params: dict | None = None):

        limit_start = 0
        data = []
        while len(data) == limit_start:
            params = params.copy() if params else {}
            params["limit"] = self.config.limit_page_length
            params["limit_start"] = limit_start
            params["fields"] = '["*"]'
            res = self.get_data(doc_type, filters=filters, params=params)
            if res:
                more_data = res.get("data")
                if isinstance(more_data, list):
                    data = data + more_data
            limit_start = limit_start + self.config.limit_page_length
        return {"data": data}

    def delete(self, doc_type: str, doc_name: str):
        endpoint = self.get_endpoint(doc_type, doc_name)
        try:
            response = requests.delete(endpoint, headers=self.headers)
            response.raise_for_status()
            logging.debug(f"{doc_name} erfolgreich gelöscht. ({endpoint})")
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Fehler beim Löschen von {doc_name} ({endpoint}): {e}")
            return None

    def get_endpoint(self, doc_type: str, doc_name: str | None = None):
        endpoint = f"{self.config.url}/api/resource/{doc_type}"
        if doc_name:
            endpoint = endpoint + f"/{doc_name}"
        return endpoint

    def get_time_zone(self):
        system_settings = self.get_data("System Settings", "System Settings").get("data")
        if system_settings:
            return system_settings["time_zone"]


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)  # or str(obj) if you prefer
        return super(CustomEncoder, self).default(obj)
