"""RAG 语料管道调度：编排 采集 → 清洗+校验 → 向量化 的每周批处理。

步骤脚本在 projects/rag-semantic-retrieval/，以 BashOperator 调用。
（BashOperator 不在项目目录、也不认 venv，故命令里写全
  cd 项目根 + venv 的 python + PySpark 所需环境变量。）
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

VENV = "/Users/thatguy/de-venv"
PY = f"{VENV}/bin/python"
RAG = "projects/rag-semantic-retrieval"


def alert_on_failure(context):
    print(f"任务失败告警: {context['task_instance'].task_id} 失败!")


default_args = {
    "owner": "thatguy",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="phase3_corpus_pipeline",
    default_args=default_args,
    description="日韩语料语义检索——离线数据管道（采集→清洗→向量化）",
    start_date=datetime(2026, 7, 1),
    schedule="0 3 * * 1",   # 每周一凌晨 3 点
    catchup=False,
    tags=["phase3", "rag"],
) as dag:
    ingest = BashOperator(
        task_id="ingest",
        bash_command=f"cd {VENV} && {PY} {RAG}/ingest.py",
    )
    clean_validate = BashOperator(
        task_id="clean_validate",
        bash_command=f"cd {VENV} && export PYSPARK_PYTHON={PY} && export PYSPARK_DRIVER_PYTHON={PY} && {PY} {RAG}/clean_validate.py",
    )
    vectorize = BashOperator(
        task_id="vectorize",
        bash_command=f"cd {VENV} && {PY} {RAG}/vectorize.py",
    )

    ingest >> clean_validate >> vectorize
