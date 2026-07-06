#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2 Day 8-9: Airflow 进阶 —— 重试 / Sensor / Hook / 告警
===============================================================

学习目标：
  1. 任务失败自动重试（retries / retry_delay）
  2. Sensor：等待条件满足再跑
  3. Hook：连接外部系统（数据库、API）
  4. 失败告警（on_failure_callback）

关键心态：
  Day 6-7 你写了一个"能跑"的 DAG。
  这两天的东西让 DAG 变成"能扛事"的——
  出错了能重试、依赖外部条件能等、出大问题能通知你。

类比：
  裸 DAG = 你让实习生手动跑脚本
  Day 8-9 的 DAG = 你设置了自动化 + 看门狗 + 报警器
"""

# ================================================================
# 第 1 关：重试机制
# ================================================================
# 概念：
#   任务失败后自动重试，不用人盯着。
#
#   两种设置方式：
#   a) default_args — 整个 DAG 的所有任务统一设置
#   b) 单个 Operator 的 retries 参数 — 覆盖默认值
#
#   关键参数：
#   retries      = 3     → 最多重试 3 次
#   retry_delay  = timedelta(minutes=5) → 每次重试间隔 5 分钟

print("===== 第 1 关：重试机制 =====")

show_retry_lesson = '''
# 方式 1：default_args 统一设置（推荐）

from datetime import timedelta

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="with_retry",
    default_args=default_args,
    ...
) as dag:
    task1 = BashOperator(task_id="t1", bash_command="...")  # 继承 3 次重试
    task2 = BashOperator(task_id="t2", bash_command="...")  # 继承 3 次重试

# 方式 2：单任务覆盖

    task3 = BashOperator(
        task_id="critical_task",
        bash_command="...",
        retries=5,           # 这个任务单独设 5 次
        retry_delay=timedelta(minutes=1),
    )
'''

print(show_retry_lesson)

# TODO 1（思考题，写注释）：
# 以下场景该设几次重试？间隔多久？
#
# a) 调用一个偶尔超时的外部 API（通常第 2 次就能成功）
#    你的答案：3 次，间隔 1 分钟
#
# b) 处理海量数据的 PySpark 任务（跑了 2 小时才失败）
#    你的答案：1 次，间隔 0 分钟（立刻重试，别让 2 小时白跑）
#
# c) 发邮件任务（发不出去就是 SMTP 挂了，重试也没用）
#    你的答案：2 次，间隔 30 秒


# ================================================================
# 第 2 关：Sensor —— 等待条件满足
# ================================================================
# 概念：
#   Sensor = 一个特殊 Operator，它不做数据处理，只"等"。
#   等到条件满足 → 返回 success，后续任务才开始。
#   超时还没等到 → 返回 failure。
#
#   类比：
#   Sensor = 餐厅门口的服务员
#   "等你们人到齐了再上菜"
#   "等那个大桌空出来再安排座位"
#
#   常用 Sensor：
#   | Sensor                     | 等什么              |
#   |-----------------------------|--------------------|
#   | FileSensor                  | 文件出现在磁盘上    |
#   | ExternalTaskSensor          | 另一个 DAG 跑完    |
#   | S3KeySensor                 | S3 文件到达        |
#   | SqlSensor                   | SQL 查询返回结果   |

print("\n===== 第 2 关：Sensor =====")

show_sensor_lesson = '''
# 场景：等一个 CSV 文件到达再开始 ETL

from airflow.providers.standard.sensors.file import FileSensor

with DAG(dag_id="etl_with_sensor", ...) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/Users/thatguy/de-venv/data/incoming/orders.csv",
        poke_interval=30,        # 每 30 秒检查一次
        timeout=60 * 60,         # 最多等 1 小时
        mode="poke",             # poke 模式（占用 slot）
    )

    process = BashOperator(
        task_id="process_file",
        bash_command="python process_orders.py",
    )

    wait_for_file >> process     # 文件到了才处理

# mode 参数：
#   "poke" — 占一个 worker slot，持续检查
#   "reschedule" — 检查一次就释放 slot，到时间再检查（省资源，推荐）
'''

print(show_sensor_lesson)


# TODO 2（思考题）：
# 以下场景用什么 Sensor？写你的判断。
#
# a) 每天等日志文件从服务器同步过来再跑分析         → FileSensor
# b) 等"数据清洗 DAG"跑完了才能跑"报表生成 DAG"    → ExternalTaskSensor
# c) 等数据库里出现至少一条新记录才开始处理           → SqlSensor


# ================================================================
# 第 3 关：Hook —— 连接外部系统
# ================================================================
# 概念：
#   Hook = Airflow 与外部的连接器。
#   它封装了认证、连接池、异常处理，你不用每次写 connection 代码。
#
#   类比：
#   Hook = 万能插头
#   - 插 MySQL → 就能操作 MySQL
#   - 插 S3 → 就能操作 S3
#   - 插 Slack → 就能发 Slack 消息
#
#   常用 Hook：
#   | Hook               | 连什么     |
#   |--------------------|-----------|
#   | MySqlHook          | MySQL     |
#   | HttpHook           | REST API  |
#   | S3Hook             | AWS S3    |
#   | SlackWebhookHook   | Slack     |

print("\n===== 第 3 关：Hook =====")

show_hook_lesson = '''
# 场景：DAG 里查询 MySQL 获取昨日订单数

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.standard.operators.python import PythonOperator

def check_yesterday_orders(**context):
    # Hook 封装了连接逻辑
    hook = DbApiHook(connection_id="mysql_default")  # connection_id 在 UI 里配置
    sql = "SELECT COUNT(*) FROM orders WHERE date = CURDATE() - INTERVAL 1 DAY"
    result = hook.get_first(sql)
    count = result[0]
    print(f"昨日订单数: {count}")
    return count

check_orders = PythonOperator(
    task_id="check_yesterday_orders",
    python_callable=check_yesterday_orders,
)

# connection_id 在哪配置？
# Web UI → Admin → Connections → 添加 MySQL 连接
# 或者在代码里用环境变量配置（生产推荐）
'''

print(show_hook_lesson)

# TODO 3：理解 Hook 的好处
# 和直接用 pymysql/sqlalchemy 相比，Hook 好在哪？
# 你的答案：
# 1) 统一管理连接信息，不用每个 DAG 写密码
# 2) 自动管理连接池，不会每个任务开一堆连接
# 3) 换数据库只改变量，不用改代码


# ================================================================
# 第 4 关：告警通知
# ================================================================
# 概念：
#   任务失败后，除了重试，还要通知人。
#   Airflow 支持的回调：
#   - on_failure_callback: 任务失败时触发
#   - on_success_callback: 任务成功时触发
#   - on_retry_callback:  任务重试时触发

print("\n===== 第 4 关：告警 =====")

show_alert_lesson = '''
from airflow.providers.standard.operators.bash import BashOperator

# ===== 4.1: 失败回调函数 =====

def on_failure_alert(context):
    """任务失败时调用"""
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    error = context["exception"]
    print(f"🚨 告警: {dag_id}.{task_id} 失败了！")
    print(f"   错误: {error}")
    # 实际工作中这里发邮件/Slack/企业微信

# ===== 4.2: 绑定到 DAG =====

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,  # ← 失败就调这个
}

with DAG(
    dag_id="with_alert",
    default_args=default_args,
    ...
) as dag:
    task1 = BashOperator(task_id="risky_task", bash_command="exit 1")
    # task1 失败 → 自动重试 3 次 → 还失败 → 调 on_failure_alert

# ===== 4.3: 单个任务级别绑定 =====

    task2 = BashOperator(
        task_id="critical",
        bash_command="...",
        on_failure_callback=critical_alert,   # 这个任务用单独的告警
    )
'''

print(show_alert_lesson)

# TODO 4: 设计一个告警策略
# 场景：你的 daily_report DAG（7 个任务，有分支）
# 哪些情况需要告警？告警给谁？
#
# 你的设计：
# 1) 任意任务失败 → on_failure_callback 发邮件/企业微信给开发
# 2) check_db 失败 → 不要重试，直接告警（数据库连不上，重试没用）
# 3) generate_report 失败 → 重试 3 次，全失败再告警（可能是数据问题）
#


# ================================================================
# 第 5 关：综合练习 —— 把之前的概念串起来
# ================================================================
# 场景：一个"等待文件 → 处理 → 写库 → 通知"的完整 DAG
#
# 要求：
#   1. FileSensor 等文件到达
#   2. PythonOperator 处理数据
#   3. 处理失败自动重试 2 次
#   4. 最终失败发通知

print("\n===== 第 5 关：综合练习 =====")
print("""
现在创建文件: ~/de-venv/airflow/dags/robust_etl.py

你的代码结构：

import sys
sys.path.insert(0, "/Users/thatguy/de-venv")

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

# 1) 告警函数
def alert_on_failure(context):
    task = context["task_instance"].task_id
    error = context["exception"]
    print(f"🚨 {task} 失败: {error}")
    # TODO: 你可以改成发邮件/写日志文件

# 2) 数据处理函数
def process_data(**context):
    print("正在处理数据...")
    # TODO: 你的真实处理逻辑
    return "done"

# 3) DAG
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="robust_etl",
    start_date=datetime(2025, 1, 1),
    schedule=None,              # 手动触发
    catchup=False,
    default_args=default_args,
    tags=["learning", "advanced"],
) as dag:

    # TODO: 写 3 个任务
    #   extract = PythonOperator(task_id="extract", python_callable=extract_data)
    #   transform = PythonOperator(task_id="transform", python_callable=process_data)
    #   load = BashOperator(task_id="load", bash_command="echo '写入数据库...'")
    #
    #   依赖：extract >> transform >> load

    pass  # 你的代码

# 验收标准：
# [ ] DAG 在 UI 里能看到
# [ ] 手动触发能跑通
# [ ] 故意让一个任务失败（bash_command="exit 1"），看它自动重试
# [ ] 重试耗尽了看到告警函数被调用
""")


# ================================================================
# 第 6 关：思考题 —— 设计一个生产级 DAG
# ================================================================
print("\n===== 第 6 关：设计题 =====")
print("""
场景：每天凌晨 2 点跑的数据管道：
  1. 等上游 A 系统的 CSV 文件到达
  2. 从 MySQL 读昨日新增用户
  3. PySpark 做聚合计算
  4. 结果写回 MySQL
  5. 发成功邮件给业务方

画出 DAG 图（用注释）：
""")

# TODO 5: 画 DAG 依赖图（箭头表示方向）
# 你的答案：
#
#csv文件到达 >> 读昨日新增用户 >> pyspark >> 结果写回 >> 发送邮件
#


# ================================================================
# 操作手册速查
# ================================================================
print("\n" + "=" * 60)
print("Day 8-9 公式速查")
print("=" * 60)
print("""
# --- 重试 ---
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# --- Sensor ---
FileSensor(
    task_id="wait",
    filepath="/path/to/file.csv",
    poke_interval=30,     # 每30秒看一次
    timeout=3600,          # 最多等1小时
    mode="reschedule",     # 推荐：省worker
)

# --- Hook ---
hook = DbApiHook(connection_id="my_mysql")
result = hook.get_first("SELECT COUNT(*) FROM t")

# --- 告警 ---
def alert(context):
    task_id = context["task_instance"].task_id
    error = context["exception"]
    # 发邮件/企业微信/Slack

default_args["on_failure_callback"] = alert
""")


# ================================================================
# 完成
# ================================================================
print("===== Phase 2 Day 8-9 完成 =====")
print("检查清单：")
print("  [ ] 理解 retries 和 retry_delay 怎么配")
print("  [ ] 知道 Sensor 是干嘛的，会用 FileSensor")
print("  [ ] 知道 Hook 封装了什么，不用每个 DAG 写密码")
print("  [ ] 会写 on_failure_callback 告警函数")
print("  [ ] 综合练习 robust_etl DAG 跑通")
print("  [ ] 能画出生产级 DAG 的流程图")
