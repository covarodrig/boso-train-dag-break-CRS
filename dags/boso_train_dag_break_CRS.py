from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException

from datetime import datetime
import requests
import json
import csv
import os
import time

logger = logging.getLogger(__name__)

API_URL = Variable.get(
    "futurama_api_url",
    default_var="https://api.sampleapis.com/futurama/characters"
)

BASE_DIR = Variable.get(
    "futurama_base_dir",
    default_var="/tmp/data_api_bad"
)


def extract_data(**context):
    execution_ts = context["execution_date"].strftime("%Y%m%d_%H%M%S")

    raw_dir = f"{BASE_DIR}/raw"
    os.makedirs(raw_dir, exist_ok=True) # Asegura que el directorio existe

    json_path = f"{raw_dir}/data_{execution_ts}.json"

    logger.info(f"Calling API: {API_URL}") # Log de la llamada a la API  

    try:
        data = response.json()
    except json.JSONDecodeError as e:
        raise AirflowException(f"API response is not valid JSON: {e}")

    if not isinstance(data, list) or not data:
        raise AirflowException("API returned an empty or invalid list")

    logger.info(f"Received {len(data)} records from API")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    logger.info(f"JSON written to {json_path}")

    return json_path # XCom para el siguiente task


def validate_json(**context):
    ti = context["ti"]

    json_path = ti.xcom_pull(task_ids="extract_data")
    if not json_path:
        raise AirflowException("No json_path found in XCom from extract_data")

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise AirflowException(f"JSON file not found: {json_path}")
    except json.JSONDecodeError as e:
        raise AirflowException(f"Invalid JSON in {json_path}: {e}")

    if not isinstance(data, list) or not data:
        raise AirflowException("JSON must be a non-empty list")

    required_fields = {"id", "name", "age"} # Validación mínima de contenido

    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise AirflowException(f"Row {i} is not a JSON object")

        missing = required_fields - row.keys()
        if missing:
            raise AirflowException(
                f"Row {i} missing required fields: {missing}"
            )

    logger.info(f"Validation OK: {len(data)} records validated")

    return True

def transform_to_csv(**context):
    ti = context["ti"]

    json_path = ti.xcom_pull(task_ids="extract_data")
    if not json_path:
        raise AirflowException("No json_path found in XCom from extract_data")

    processed_dir = f"{BASE_DIR}/processed"
    os.makedirs(processed_dir, exist_ok=True)

    base_name = os.path.basename(json_path).replace(".json", "")
    csv_path = f"{processed_dir}/{base_name}.csv"

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Adición dinámica de campos: detectar todos los campos presentes en los datos
    all_fields = set()
    for row in data:
        all_fields.update(row.keys())

    fieldnames = sorted(all_fields)

    logger.info(f"Dynamic schema detected ({len(fieldnames)} columns)")

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in data:
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    logger.info(f"CSV written to {csv_path}")

    return csv_path

def pipeline_summary(**context):
    ti = context["ti"]

    json_path = ti.xcom_pull(task_ids="extract_data")
    csv_path = ti.xcom_pull(task_ids="transform_to_csv")

    with open(csv_path, "r", encoding="utf-8") as f:
        record_count = sum(1 for _ in f) - 1  

    logger.info("========== SUMMARY ==========")
    logger.info(f"DAG Run ID: {context['run_id']}")
    logger.info(f"Execution date: {context['execution_date']}")
    logger.info(f"JSON file: {json_path}")
    logger.info(f"CSV file: {csv_path}")
    logger.info(f"Records processed: {record_count}")


dag = DAG(
    dag_id="etl_api_bad_practices",
    description="INTENTIONALLY bad DAG for training",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

extract = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag,
)

validate = PythonOperator(
    task_id="validate_json",
    python_callable=validate_json,
    dag=dag,
)

t_transform = PythonOperator(
    task_id="transform_to_csv",
    python_callable=transform_to_csv,
    dag=dag,
)

summary = PythonOperator(
    task_id="pipeline_summary",
    python_callable=pipeline_summary,
    dag=dag,
)

extract >> validate >> t_transform >> summary

