from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="my_first_etl",              # DAG 的唯一标识
    start_date=datetime(2025, 1, 1),    # 从这个日期开始调度
    schedule="@daily",                  # 每天跑一次
    catchup=False,                      # 不补跑历史任务
    tags=["learning", "etl"],           # 在 UI 里可以按标签筛选
) as dag:

    # TODO: 创建三个任务
    extract = BashOperator(
        task_id="extract",
        bash_command="echo '📦 正在提取数据...'",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="echo '🔧 正在转换数据...'",
    )

    load = BashOperator(
        task_id="load",
        bash_command="echo '📥 正在加载数据...'",
    )

    # TODO: 设置执行顺序
    extract >> transform >> load