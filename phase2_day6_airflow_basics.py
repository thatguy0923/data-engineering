#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2 Day 6-7: Airflow —— 工作流调度引擎
==================================================

学习目标：
  1. 理解 DAG、Operator、Task 是什么
  2. 读懂一个 DAG 文件的结构
  3. 亲手写一个完整的 DAG 并运行

关键心态：
  Airflow 不是用来"算数据"的（那是 PySpark 的活），
  Airflow 是用来"安排活"的——什么任务、按什么顺序、什么时间跑。

  类比：Airflow = 餐厅的厨师长
  - 他不亲自炒菜（PySpark 炒）
  - 但他决定：先备菜 → 再炒菜 → 最后装盘
  - 哪道菜出问题，他重新炒
  - 每天晚上 6 点准时开灶

---

不同于之前的 PySpark —— 这次你需要 3 个终端协作:

  终端 1: Airflow 调度器 + Web 服务器（已启动，运行 `airflow standalone`）
  终端 2: 写 DAG 文件（你的代码）
  终端 3: 触发 DAG 运行 + 看日志

Airflow Web UI: http://localhost:8080
  用户名: admin
  密码:  dXrdxMtn7A7XUuFr

"""

# ================================================================
# 第 1 关：理解 DAG（有向无环图）
# ================================================================
# 概念：
#   DAG = Directed Acyclic Graph = 有向无环图
#
#   拆开理解：
#   - Directed（有向）：任务有方向，A 做完才能做 B，不能反过来
#   - Acyclic（无环）：不能有循环 A→B→C→A，那会死锁
#   - Graph（图）：任务和它们之间的连线
#
#   现实类比：
#   做蛋糕的 DAG：
#     买材料 → 混合面糊 → 预热烤箱 → 烤 → 冷却 → 装饰
#                      ↘
#                       打奶油 ↗
#
#   有方向：先买材料才能混合，不能反过来
#   无环：不能"装饰完了再去买材料"形成循环

print("===== 第 1 关：DAG 概念 =====")
print("回答以下问题（在注释里写答案）：")

# TODO 1: 以下哪些是 DAG，哪些不是？写 '是' 或 '否' 并说明原因
#
# a) 起床 → 刷牙 → 吃早餐 → 出门 → 开会
#    你的答案：是
#
# b) 步骤A → 步骤B → 步骤C → 步骤A
#    你的答案：否，因为形成了一个循环定义死锁
#
# c) 加载数据 → 清洗数据 → 检查质量 → 写入数据库
#    你的答案：是一个明确的流程没有循环
#
# d) 任务1 → 任务2 → 任务3
#    任务1 → 任务3
#    你的答案：是，因为最终的方向是从1走向3
#
# e) 编译代码 → 跑测试 → 部署
#    跑测试没通过 → 回到改代码（循环）
#    你的答案：不是，照这样可能能一直循环

print("→ 写完答案后继续第 2 关\n")


# ================================================================
# 第 2 关：Operator —— 任务的类型
# ================================================================
# 概念：
#   Operator = 一个具体任务的模板
#   你告诉 Airflow "做什么"，Operator 决定"怎么做"
#
#   三种最常用的 Operator：
#
#   | Operator         | 做什么               | 什么时候用         |
#   |------------------|----------------------|-------------------|
#   | BashOperator     | 执行 shell 命令      | 跑脚本、调系统命令  |
#   | PythonOperator   | 执行 Python 函数     | 数据处理、API 调用  |
#   | EmptyOperator    | 什么都不做            | 占位、标记起止点   |
#
#   类比：
#   BashOperator = 你对厨师说"切菜"（用刀）
#   PythonOperator = 你对厨师说"炒菜"（用锅）
#   EmptyOperator = 在菜单上画一条分割线

print("===== 第 2 关：认识 Operator =====")

# TODO 2: 为以下场景选择合适的 Operator（写 Bash 或 Python）
#
# a) 每天凌晨 3 点运行一个 PySpark 脚本    → 你的答案：Bash
# b) 调用一个 REST API 获取天气数据        → 你的答案：Python
# c) 删除 /tmp 下超过 7 天的临时文件       → 你的答案：Bash
# d) 从 MySQL 读数据，清洗后写回 MySQL     → 你的答案：Python
# e) 标记"ETL 流程结束"                    → 你的答案：Empty

print("→ 写完答案后继续第 3 关\n")


# ================================================================
# 第 3 关：看懂 DAG 文件的结构
# ================================================================
# 一个最小化的 DAG 文件长这样：
#
#   from airflow import DAG
#   from airflow.providers.standard.operators.bash import BashOperator
#   from datetime import datetime
#
#   with DAG(
#       dag_id="my_first_dag",    # DAG 的唯一名字
#       start_date=datetime(2025, 1, 1),  # 从哪天开始调度
#       schedule="@daily",        # 多久跑一次
#       catchup=False,            # 不补跑过去没跑的
#       tags=["tutorial"],        # 标签，在 UI 里筛选用
#   ) as dag:
#
#       task1 = BashOperator(
#           task_id="say_hello",
#           bash_command="echo 'Hello Airflow!'",
#       )
#
#       task2 = BashOperator(
#           task_id="say_goodbye",
#           bash_command="echo 'Goodbye!'",
#       )
#
#       task1 >> task2   # task1 跑完才能跑 task2
#
#   结构拆解：
#   1. with DAG(...) as dag: → 创建一个 DAG 容器
#   2. task1 = ...Operator(...) → 定义任务
#   3. task1 >> task2 → 设置依赖方向

print("===== 第 3 关：理解 DAG 结构 =====")

# TODO 3: 写出下列 DAG 文件的执行顺序
# 阅读下面的 DAG 定义，写出任务执行顺序（用 → 连接）
#
# with DAG(dag_id="etl_pipeline", ...) as dag:
#     extract = BashOperator(task_id="extract", ...)
#     transform = PythonOperator(task_id="transform", ...)
#     load = BashOperator(task_id="load", ...)
#     validate = PythonOperator(task_id="validate", ...)
#     notify = BashOperator(task_id="notify", ...)
#
#     extract >> transform >> load >> validate >> notify
#
# 你的答案：extract → transform → load → validate → notify

# TODO 4: 下面这个 DAG 的执行顺序是什么？
#
#     start >> task_a >> task_c >> end
#     start >> task_b >> task_c
#
# 你的答案：task_a和task_b同时>>task_c>>end

print("→ 写完答案后继续第 4 关\n")


# ================================================================
# 第 4 关：亲手写第一个 DAG 文件
# ================================================================
# 现在你要在 ~/de-venv/airflow/dags/ 下创建一个真正的 DAG 文件。
#
# 场景：一个简单的 ETL 流水线
#   1. 提取数据（extract）：打印 "正在提取数据..."
#   2. 转换数据（transform）：打印 "正在转换数据..."
#   3. 加载数据（load）：打印 "正在加载数据..."
#   4. 任务按顺序执行：extract → transform → load
#
# 文件路径：~/de-venv/airflow/dags/my_first_etl.py

print("===== 第 4 关：写 DAG 文件 =====")
print("现在切换到编辑器，创建文件：")
print("  ~/de-venv/airflow/dags/my_first_etl.py")
print("")
print("模板已经给你放在下方。复制过去，把 TODO 补上。")
print("")

dags_dir = "/Users/thatguy/de-venv/airflow/dags"

# 提示用户创建的 DAG 文件内容:
dag_template = '''
# TODO 4: 补全这个 DAG 文件
# 把它保存到 ~/de-venv/airflow/dags/my_first_etl.py

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

# 你的任务：
# 1. 创建 DAG，dag_id="my_first_etl"
# 2. 设置 schedule="@daily"（每天跑一次）
# 3. 创建三个 BashOperator 任务：extract, transform, load
# 4. 设置依赖：extract >> transform >> load

# --- 模板开始 ---

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

# --- 模板结束 ---
#
# 看完后在终端运行验证：
#   export PATH="/Users/thatguy/de-venv/bin:$PATH"
#   export AIRFLOW_HOME=~/de-venv/airflow
#   airflow dags list          # 查看 DAG 是否被 Airflow 发现
#   airflow dags trigger my_first_etl   # 手动触发运行
'''

print(dag_template)

print("\n→ 创建完文件后回来继续第 5 关")


# ================================================================
# 第 5 关：BashOperator vs PythonOperator
# ================================================================
# BashOperator 执行 shell 命令，但数据处理通常用 Python 写。
# 这时候用 PythonOperator —— 直接调用 Python 函数。

print("\n===== 第 5 关：PythonOperator =====")

# TODO 5: 看懂下面的 PythonOperator 用法，回答问题
#
# from airflow.providers.standard.operators.python import PythonOperator
#
# def clean_data():
#     """清洗数据函数"""
#     print("开始清洗数据...")
#     # 这里写真正的清洗逻辑
#     cleaned_count = 1000
#     print(f"清洗完成，共 {cleaned_count} 行")
#     return cleaned_count
#
# clean_task = PythonOperator(
#     task_id="clean_data",
#     python_callable=clean_data,  # ← 注意：传函数名，不是调用 clean_data()
# )
#
# 问题 a) python_callable 传的是 clean_data 还是 clean_data()？为什么？
#
# 你的答案：clean_data。你要交给airflow执行的函数，而不用加（）是因为如果在这加（）
# 这个函数就会立即执行，不符合要求
#
# 问题 b) 如果函数需要参数怎么办？（提示：用 op_kwargs）
#
# 你的答案：在pythonoperator最下方的括号里加入op_kwargs={"参数名":"值"}

print("→ 写完答案后继续第 6 关\n")


# ================================================================
# 第 6 关：更复杂的任务依赖
# ================================================================
# 现实中的 DAG 不是一条直线，经常有分支和汇合：

print("===== 第 6 关：复杂依赖 =====")

# TODO 6: 画出下面 DAG 的执行顺序
#
#                      ┌─→ branch_a ─→ process_a ─┐
#                      │                          │
#     start ─→ split ─┤                          ├─→ merge ─→ finish
#                      │                          │
#                      └─→ branch_b ─→ process_b ─┘
#
# 用 >> 符号写出依赖关系（你的答案）：

# 你的代码（用注释写出即可）
# start >> split
# split >> branch_a >> process_a >> merge
# split >> branch_b >> process_b >> merge
# merge >> finish

# TODO 7: 思考题
# branch_a 和 branch_b 可以同时运行吗？
# 你的答案：可以
#
# merge 必须等 branch_a 和 branch_b 都完成才能开始吗？
# 你的答案：是的


# ================================================================
# 第 7 关：综合练习 —— 写一个真实场景的 DAG
# ================================================================
# 场景：数据分析日报流水线
#
# 每天早上 8 点：
#   1. check_db：检查数据库是否在线
#   2. extract_orders：从数据库提取昨日订单
#   3. extract_users：从数据库提取昨日新增用户
#   4. calculate_metrics：等订单和用户都提取完，计算指标
#   5. generate_report：生成日报 HTML
#   6. send_email：发送邮件给老板
#
# 画成图：
#                      ┌─→ extract_orders ─┐
#   check_db ─→       │                     ├─→ calculate_metrics ─→ generate_report ─→ send_email
#                      └─→ extract_users  ─┘

print("\n===== 第 7 关：综合练习 =====")
print("创建文件 ~/de-venv/airflow/dags/daily_report.py")
print("要求：用 BashOperator 模拟每个步骤（打印一句话即可）")

# TODO 8: 写完整的 daily_report DAG
daily_report_template = '''
# 文件：~/de-venv/airflow/dags/daily_report.py
# 场景：每天早上 8 点生成数据分析日报

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="daily_report",
    start_date=datetime(2025, 1, 1),
    schedule="0 8 * * *",  # 每天早上 8 点（cron 表达式）
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

# --- 模板结束 ---
'''

print(daily_report_template)

print("\n验收标准：")
print("  [ ] DAG 在 UI 里能看到（http://localhost:8080）")
print("  [ ] 手动触发能跑通")
print("  [ ] 所有任务状态是绿色（success）")


# ================================================================
# 操作手册
# ================================================================
print("\n" + "=" * 60)
print("Airflow 常用命令")
print("=" * 60)
print("""
# 查看所有 DAG
airflow dags list

# 手动触发一个 DAG 运行
airflow dags trigger my_first_etl

# 查看 DAG 的任务列表
airflow tasks list my_first_etl

# 测试单个任务（不触发整个 DAG）
airflow tasks test my_first_etl extract 2025-01-01

# 查看运行历史
airflow dags list-runs -d my_first_etl

# Web UI
http://localhost:8080 (admin / dXrdxMtn7A7XUuFr)
""")


# ================================================================
# 完成
# ================================================================
print("===== Phase 2 Day 6-7 完成 =====")
print("检查清单：")
print("  [ ] 理解 DAG 是什么（有向无环图）")
print("  [ ] 知道 BashOperator 和 PythonOperator 的区别")
print("  [ ] 能读懂 DAG 文件的 with DAG / task / >> 结构")
print("  [ ] 亲手写了一个 my_first_etl DAG 并在 UI 看到它")
print("  [ ] 亲手写了 daily_report DAG 并跑通")
print("  [ ] 理解 schedule 怎么设置（@daily / cron 表达式）")
print("  [ ] 理解分支依赖 [t1, t2] >> t3 的写法")
