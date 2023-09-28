from time import sleep
import requests
import logging
from dotenv import load_dotenv
from os import getenv

load_dotenv()

airflow_web_host = getenv("HOST")
airflow_web_port = getenv("WEBSERVER_PORT")
load_dag_name = getenv("FACT_LOAD_DAG_NAME")
username = getenv("AIRFLOW_ADMIN_USERNAME")
password = getenv("AIRFLOW_ADMIN_PASSWORD")

api_url = "http://{host}:{port}/api/v1/"
trigger_path = "dags/{dag_id}/dagRuns"
monitor_path = "dags/{dag_id}/dagRuns/{dag_run_id}"

base_url = api_url.format(host=airflow_web_host, port=airflow_web_port)
trigger_endpoint = base_url + trigger_path.format(dag_id=load_dag_name)

# cr. https://stackoverflow.com/questions/28330317/print-timestamp-for-logging-in-python
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)


def is_dag_complete(dag_id: str, run_id: str):
    monitor_endpoint = base_url + monitor_path.format(dag_id=dag_id, dag_run_id=run_id)
    response = requests.get(monitor_endpoint, auth=(username, password))
    assert response.status_code == 200
    return response.json().get("end_date")


if __name__ == "__main__":
    response = requests.post(trigger_endpoint, json={}, auth=(username, password))
    assert response.status_code == 200
    run_id = response.json()["dag_run_id"]

    while True:
        if is_dag_complete(load_dag_name, run_id):
            break
        logging.info("... waiting for dag " + load_dag_name + " completion ...")
        sleep(3)
