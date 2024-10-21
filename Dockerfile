FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y unixodbc libfbclient2

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python", "./sync.py"]