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


def quality_check(**context):
    """GX 数据质量验证：直接连 MySQL 的结果表，不用跨任务传 df。"""
    import great_expectations as gx
    from great_expectations.expectations import (
        ExpectColumnMinToBeBetween,
        ExpectColumnValuesToBeBetween,
        ExpectColumnValuesToNotBeNull,
    )

    ctx = gx.get_context(mode="ephemeral")
    # add_sql：直接连库，数据已经在表里（run_etl 那步写进去的）
    ds = ctx.data_sources.add_sql(
        name="refund_sql",
        connection_string="mysql+pymysql://root@localhost:3306/refund_db",
    )
    asset = ds.add_table_asset(name="refund_table", table_name="daily_refund_report")
    bd = asset.add_batch_definition_whole_table("whole")

    suite = ctx.suites.add(gx.ExpectationSuite(name="refund_suite"))
    suite.add_expectation(ExpectColumnMinToBeBetween(column="total_orders", min_value=1))
    suite.add_expectation(ExpectColumnValuesToBeBetween(column="total_refund", min_value=0))
    suite.add_expectation(ExpectColumnValuesToBeBetween(column="refund_rate", min_value=0, max_value=1))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="product_category"))

    vd = ctx.validation_definitions.add(
        gx.ValidationDefinition(name="refund_vd", data=bd, suite=suite))
    result = vd.run()  # SQL 方式不用传 batch_parameters

    if not result.success:
        stats = result.statistics
        raise ValueError(
            f"数据质量验证失败: {stats['unsuccessful_expectations']}/{stats['evaluated_expectations']} 条未通过"
        )

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

    quality_check_task = PythonOperator(
        task_id = "quality_check",
        python_callable = quality_check,
    )

    run_etl >> quality_check_task