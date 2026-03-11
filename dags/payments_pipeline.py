from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys

default_args = {
    "owner": "dataops",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def generate_transactions():
    import subprocess
    result = subprocess.run(
        [sys.executable, "data/generate_transactions.py"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(result.stderr)

def run_validation():
    import subprocess
    result = subprocess.run(
        [sys.executable, "tests/validate_data.py"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Validation failed:\n{result.stderr}")

with DAG(
    dag_id="payments_daily_pipeline",
    description="Daily payments data pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * 1-5",  # 6am UTC weekdays
    catchup=False,
    tags=["payments", "dataops"]
) as dag:

    t1 = PythonOperator(
        task_id="generate_transactions",
        python_callable=generate_transactions,
    )

    t2 = BashOperator(
        task_id="dbt_run",
        bash_command="cd {{ var.value.get('project_dir', '.') }}/dbt_project/payments && dbt run --profiles-dir .",
    )

    t3 = BashOperator(
        task_id="dbt_test",
        bash_command="cd {{ var.value.get('project_dir', '.') }}/dbt_project/payments && dbt test --profiles-dir .",
    )

    t4 = PythonOperator(
        task_id="great_expectations_validation",
        python_callable=run_validation,
    )

    t1 >> t2 >> t3 >> t4