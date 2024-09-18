import logging


class SyncTask:
    def __init__(self, task_config, db_conn, frappe_api):
        self.endpoint = task_config['endpoint']
        self.query = task_config['query']
        self.mapping = task_config['mapping']
        self.db_type = task_config['db_type']
        self.direction = task_config.get('direction', 'db_to_frappe')
        self.key_field = task_config.get('key_field')
        self.db_conn = db_conn.get_connection(self.db_type)
        self.frappe_api = frappe_api

    def execute(self):
        if self.direction == 'db_to_frappe':
            self.sync_db_to_frappe()
        elif self.direction == 'frappe_to_db':
            self.sync_frappe_to_db()
        else:
            logging.error(f"Unbekannte Synchronisationsrichtung: {self.direction}")

    def sync_db_to_frappe(self):
        cursor = self.db_conn.cursor()
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
            if self.key_field and self.key_field in data:
                doc_name = data[self.key_field]
                endpoint = f"{self.endpoint}/{doc_name}"
                # Prüfen, ob das Dokument existiert
                existing_doc = self.frappe_api.get_data(endpoint)
                if existing_doc:
                    method = 'PUT'
                else:
                    method = 'POST'
                    endpoint = self.endpoint  # Zurücksetzen auf den Endpunkt ohne doc_name
            else:
                method = 'POST'

            # Daten an Frappe senden
            self.frappe_api.send_data(method, endpoint, data)

        cursor.close()

    def sync_frappe_to_db(self):
        # Daten von Frappe abrufen
        response = self.frappe_api.get_data(self.endpoint)
        if not response:
            return

        data_list = response.get('data', [])
        cursor = self.db_conn.cursor()

        for data in data_list:
            # Mapping umkehren: Frappe-Felder zu DB-Spalten
            db_data = {}
            for frappe_field, db_column in self.mapping.items():
                if frappe_field in data:
                    db_data[db_column] = data[frappe_field]
                else:
                    logging.warning(f"Feld {frappe_field} nicht in Frappe-Daten gefunden.")

            # Prüfen, ob der Datensatz bereits existiert
            if self.key_field and self.key_field in data:
                key_value = data[self.key_field]
                # Annahme: Der Tabellenname ist in self.query angegeben
                table_name = self.query
                # Überprüfen, ob der Datensatz existiert
                select_sql = f"SELECT COUNT(*) FROM {table_name} WHERE {self.key_field} = ?"
                cursor.execute(select_sql, key_value)
                exists = cursor.fetchone()[0] > 0

                if exists:
                    # Update
                    set_clause = ', '.join([f"{col} = ?" for col in db_data.keys()])
                    update_sql = f"UPDATE {table_name} SET {set_clause} WHERE {self.key_field} = ?"
                    params = list(db_data.values()) + [key_value]
                    try:
                        cursor.execute(update_sql, params)
                        self.db_conn.commit()
                        logging.info(f"Datensatz mit Schlüssel {key_value} erfolgreich aktualisiert.")
                    except Exception as e:
                        logging.error(f"Fehler beim Aktualisieren des Datensatzes: {e}")
                        self.db_conn.rollback()
                else:
                    # Insert
                    placeholders = ', '.join(['?'] * len(db_data))
                    columns = ', '.join(db_data.keys())
                    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    try:
                        cursor.execute(insert_sql, list(db_data.values()))
                        self.db_conn.commit()
                        logging.info(f"Datensatz mit Schlüssel {key_value} erfolgreich eingefügt.")
                    except Exception as e:
                        logging.error(f"Fehler beim Einfügen des Datensatzes: {e}")
                        self.db_conn.rollback()
            else:
                logging.warning("Kein Schlüsselwert für den Datensatz gefunden. Überspringe Datensatz.")

        cursor.close()