# airflow on docker

```bash
mkdir airflow_on_docker
cd airflow_on_docker
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
code .
mkdir ./dags ./logs ./plugins ./data
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

#### Built docker image to install libraries from a requirements.txt 
```bash
docker build -t apache/airflow:2.9.3 . 
```

#### Initialize the database and create a user
```bash
docker compose up airflow-init
```

#### Start all services
```bash
docker compose up
```

#### Stop , delete containers , delete volumes with database data and download images
```bash
docker compose down --volumes --rmi all
```

### ref
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
