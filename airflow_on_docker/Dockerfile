FROM apache/airflow:2.9.3
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /tmp/requirements.txt
