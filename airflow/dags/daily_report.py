from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="daily_report",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",  # 每天早上 8 点（cron 表达式）
    catchup=False,
    tags=["learning", "report"],
) as dag:

    # TODO: 创建 6 个任务，每个用 BashOperator + echo
    #
    # 提示：
    #   check_db = BashOperator(task_id="check_db", bash_command="echo '检查数据库连接...'")
    #   ... 继续写

    # 你的代码：
    check_db = BashOperator(
        task_id="check_db",
        bash_command="echo '🔍 检查数据库连接...'"
    )

    extract_orders = BashOperator(
        task_id="extract_orders",
        bash_command="echo '📦 提取昨日订单...'"
    )

    extract_users = BashOperator(
        task_id="extract_users",
        bash_command="echo '👤 提取昨日新增用户...'"
    )

    calculate_metrics = BashOperator(
        task_id="calculate_metrics",
        bash_command="echo '📊 计算核心指标...'"
    )

    generate_report = BashOperator(
        task_id="generate_report",
        bash_command="echo '📄 生成日报HTML...'"
    )

    send_email = BashOperator(
        task_id="send_email",
        bash_command="echo '📧 发送邮件给老板...'"
    )

    # TODO: 设置依赖关系
    # 提示：
    #   check_db >> [extract_orders, extract_users]
    #   [extract_orders, extract_users] >> calculate_metrics
    #   ...

    # 你的代码：
    check_db >> [extract_orders, extract_users]
    [extract_orders, extract_users] >> calculate_metrics
    calculate_metrics >> generate_report
    generate_report >> send_email
