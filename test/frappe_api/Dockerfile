# test_frappe_api/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Installiere Abhängigkeiten
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kopiere den Code in das Image
COPY app.py .

EXPOSE 5000

CMD ["python", "app.py"]
