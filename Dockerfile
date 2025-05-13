FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y unixodbc libfbclient2 gnupg curl ca-certificates apt-transport-https software-properties-common cron

COPY install_ms_sql_driver.sh /tmp/install_ms_sql_driver.sh
RUN chmod +x /tmp/install_ms_sql_driver.sh && \
    /tmp/install_ms_sql_driver.sh && \
    rm /tmp/install_ms_sql_driver.sh

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]
