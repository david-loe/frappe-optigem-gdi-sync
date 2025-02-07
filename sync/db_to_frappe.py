import logging

import urllib
from sync.task import SyncTaskBase


class DbToFrappeSyncTask(SyncTaskBase):
    name = "DB -> Frappe"

    def validate_config(self):
        required_fields = ["endpoint", "query", "mapping", "db_name"]
        missing_fields = [field for field in required_fields if not hasattr(self, field)]
        if missing_fields:
            raise ValueError(
                f"Fehlende erforderliche Konfigurationsfelder für 'db_to_frappe': {', '.join(missing_fields)}"
            )
        if not self.key_fields:
            logging.warning("Keine 'key_fields' definiert. Es können keine Updates durchgeführt werden, nur Inserts.")

    def sync(self):
        logging.info(f"Starte Ausführung von '{self.name}'.")

        cursor = self.db_conn.cursor()
        logging.debug(
            f"""Anfrage an {self.db_name}
                {self.query}"""
        )
        cursor.execute(self.query)
        columns = [column[0] for column in cursor.description]

        for row in cursor.fetchall():
            data = {}
            for frappe_field, db_column in self.mapping.items():
                if db_column in columns:
                    data[frappe_field] = row[columns.index(db_column)]
                else:
                    logging.warning(f"Spalte {db_column} nicht im Ergebnis gefunden.")

            # Überprüfen, ob das Dokument bereits existiert
            filters = []
            for key_field in self.key_fields:
                if key_field in data:
                    filters.append(f'["{key_field}", "=", "{data[key_field]}"]')
                else:
                    logging.warning(f"Schlüsselfeld {key_field} nicht in Daten gefunden.")

            if filters:
                filters_str = urllib.parse.quote(f"[{','.join(filters)}]")
                endpoint = f"{self.endpoint}?filters={filters_str}"

                # Suche nach existierendem Dokument
                existing_docs = self.frappe_api.get_data(endpoint)
                if existing_docs and existing_docs.get("data"):
                    # Dokument(e) existieren
                    if self.process_all:
                        # Verarbeite alle gefundenen Dokumente
                        for doc in existing_docs["data"]:
                            doc_name = doc["name"]
                            method = "PUT"
                            doc_endpoint = f"{self.endpoint}/{doc_name}"
                            # Daten an Frappe senden
                            self.frappe_api.send_data(method, doc_endpoint, data)
                        continue  # Gehe zur nächsten Zeile in der Schleife
                    else:
                        # Verarbeite nur das erste gefundene Dokument
                        doc_name = existing_docs["data"][0]["name"]
                        method = "PUT"
                        endpoint = f"{self.endpoint}/{doc_name}"
                else:
                    method = "POST"
                    endpoint = self.endpoint  # Zurücksetzen auf den Endpunkt ohne doc_name
            else:
                method = "POST"
                endpoint = self.endpoint

            if self.create_new or method == "PUT":
                # Daten an Frappe senden
                self.frappe_api.send_data(method, endpoint, data)

        cursor.close()
