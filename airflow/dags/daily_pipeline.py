import subprocess
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

def alert(context):
    task_id = context["task_instance"].task_id
    error = context["exception"]
    print(f"🚨 {task_id} 失败: {error}")

def run_step(step, **context):
    """调 pipeline_etl.py 的指定 step"""
    logical_date = context["dag_run"].logical_date
    if logical_date:
        date_str = logical_date.strftime("%Y-%m-%d")
    else:
        from datetime import date
        date_str = date.today().isoformat()  # 手动触发用今天
    cmd = [
        "/Users/thatguy/de-venv/bin/python",
        "/Users/thatguy/de-venv/pipeline_etl.py",
        "--step", step,
        "--date", date_str,
    ]
    subprocess.run(cmd, check=True)

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert,
}

with DAG(
    dag_id="daily_pipeline",
    start_date=datetime(2025, 6, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["production", "pipeline"],
) as dag:

    run_etl = PythonOperator(
        task_id="run_etl",
        python_callable=run_step,
        op_kwargs={"step": "all"},
    )

    run_etl
