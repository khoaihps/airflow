FROM apache/airflow:2.10.5

USER root
RUN apt-get update && apt-get install -y gcc python3-dev

USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
