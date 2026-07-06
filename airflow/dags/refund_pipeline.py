import subprocess

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

def alert(context):
    task_id = context["task_instance"].task_id
    error = context["exception"]
    print(f"{task_id} 失败: {error}")

default_args = {
    "retries" : 2,
    "retry_delay" : timedelta(minutes=5),
    "on_failure_callback" : alert,
}

def run_step(step, **context):
    logical_date = context["dag_run"].logical_date
    if logical_date:
        date_str = logical_date.strftime("%Y-%m-%d")
    else:
        from datetime import date
        date_str = date.today().isoformat()
    cmd = [
        "/Users/thatguy/de-venv/bin/python",
        "/Users/thatguy/de-venv/refund_etl.py",
        "--step", step,
        "--date", date_str,
    ]
    subprocess.run(cmd, check=True)

with DAG(
    dag_id = "refund_pipeline",
    start_date = datetime(2025, 1, 1),
    schedule = "0 3 * * *",
    catchup = False,
    default_args= default_args,
    tags = ["report", "refund"],
) as dag:
    
    run_etl = PythonOperator(
        task_id = "run_etl",
        python_callable = run_step,
        op_kwargs = {"step": "all"},
    )