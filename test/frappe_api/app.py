import json
from flask import Flask, request, jsonify, abort
import uuid  # Importiere das Modul für UUIDs

app = Flask(__name__)

# In-Memory Datenspeicher: Simuliert Dokumente für verschiedene "DocTypes"
data_store = {"Contact": {}}  # Schlüssel: Dokumentname, Wert: Datensatz


@app.route("/api/resource/<doctype>", methods=["GET"])
def get_resource(doctype):
    """
    Unterstützt folgende Query-Parameter:
      - fields: JSON-kodierte Liste von Feldern (z.B. ?fields=["description", "name"])
                Falls fields=["*"] übergeben wird, werden alle Felder zurückgegeben.
      - limit_start: Startindex (default: 0)
      - limit_page_length: Anzahl Datensätze pro Seite (default: 20)

    Ohne den Parameter "fields" werden nur das "name"-Feld der Datensätze zurückgegeben.
    """
    # Parameter aus der URL abrufen
    fields_param = request.args.get("fields")

    try:
        limit_start = int(request.args.get("limit_start", 0))
    except ValueError:
        abort(400, description="limit_start muss eine Zahl sein")

    try:
        limit_page_length = int(request.args.get("limit_page_length", 20))
    except ValueError:
        abort(400, description="limit_page_length muss eine Zahl sein")

    # Alle Datensätze für den angefragten doctype abrufen
    records = list(data_store.get(doctype, {}).values())

    # Paginierung anwenden
    records = records[limit_start : limit_start + limit_page_length]

    # Falls ein "fields"-Parameter angegeben wurde, selektiere nur diese Felder
    if fields_param:
        try:
            fields = json.loads(fields_param)
            if not isinstance(fields, list):
                raise ValueError
        except ValueError:
            abort(400, description="Ungültiger 'fields'-Parameter. Erwartet wird eine JSON-kodierte Liste.")

        if fields == ["*"]:
            # Alle Felder werden zurückgegeben
            filtered_records = records
        else:
            filtered_records = []
            for rec in records:
                filtered_record = {}
                for field in fields:
                    filtered_record[field] = rec.get(field)
                filtered_records.append(filtered_record)
        records = filtered_records
    else:
        # Ohne "fields"-Parameter: Rückgabe nur des "name"-Feldes
        records = [{"name": rec.get("name")} for rec in records]

    return jsonify({"data": records})


@app.route("/api/resource/<doctype>/<name>", methods=["GET"])
def get_resource_item(doctype, name):
    """
    Liefert den vollständigen Datensatz eines Dokuments, wenn anhand des doctype und Namens gefunden.
    Andernfalls wird ein 404-Fehler zurückgegeben.
    """
    record = data_store.get(doctype, {}).get(name)
    if not record:
        abort(404, description="Dokument nicht gefunden")
    return jsonify({"data": record})


@app.route("/api/resource/<doctype>", methods=["POST"])
def create_resource(doctype):
    record = request.get_json(force=True)
    if not record:
        abort(400, description="Kein JSON Payload übermittelt")
    # Generiere eine UUID, falls kein "name" Feld vorhanden ist
    if "name" not in record or record["name"] == None:
        record["name"] = str(uuid.uuid4())
    data_store.setdefault(doctype, {})[record["name"]] = record
    return jsonify({"data": record}), 201


@app.route("/api/resource/<doctype>/<docname>", methods=["PUT"])
def update_resource(doctype, docname):
    if doctype not in data_store or docname not in data_store[doctype]:
        abort(404, description="Dokument nicht gefunden")
    record = request.get_json(force=True)
    if not record:
        abort(400, description="Kein JSON Payload übermittelt")
    record["name"] = docname
    data_store[doctype][docname] = record
    return jsonify({"data": record})


@app.route("/api/resource/<doctype>/<docname>", methods=["DELETE"])
def delete_resource(doctype, docname):
    if doctype not in data_store or docname not in data_store[doctype]:
        abort(404, description="Dokument nicht gefunden")
    record = data_store[doctype].pop(docname)
    return jsonify({"message": "ok"})


if __name__ == "__main__":
    # Damit die App in Docker auf alle Schnittstellen horcht
    app.run(host="0.0.0.0", port=5000)
