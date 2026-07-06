#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2 Day 10-11: 完整数据管道 —— 面试核心项目
==================================================

学习目标：
  1. 把 PySpark + MySQL + Airflow 串成一条生产线
  2. 理解"数据管道"不只是一段代码，而是一个流程
  3. 产出可以直接写进简历的项目

场景：电商订单日报管道
  每日凌晨跑一次：
    原始订单 CSV → PySpark 清洗+聚合 → 写 MySQL → 数据验证 → 邮件通知

整体架构：
                           ┌───────────┐
   orders.csv ──→   PySpark ETL    │
                           │  · 清洗脏数据      │
                           │  · 按日期/品类聚合  │
                           │  · 计算核心指标     │
                           └─────┬─────┘
                                 │
                                 ▼
                           ┌───────────┐
                           │   MySQL    │
                           │  三张表：   │
                           │  · daily_sales │
                           │  · daily_users │
                           │  · pipeline_log│
                           └─────┬─────┘
                                 │
                                 ▼
                           ┌───────────┐
                           │  验证      │
                           │  · 行数检查 │
                           │  · 金额非负 │
                           │  · 日期连续 │
                           └───────────┘
"""

# ================================================================
# 第 1 关：理解管道的三个部分
# ================================================================
print("===== 第 1 关：管道架构 =====")

print("""
一条完整管道 = 三样东西捆在一起:

  ┌──────────────────────────────────────────────────────┐
  │ 1. 数据处理脚本（PySpark）                            │
  │    - 读数据、洗数据、算指标                           │
  │    - 单机可跑，不依赖 Airflow                         │
  │    - 就是你之前写的 phase2_day4_*.py 那种             │
  ├──────────────────────────────────────────────────────┤
  │ 2. 目标存储（MySQL）                                 │
  │    - 存结果，业务方查询                               │
  │    - 存管道运行日志（跑了多久、多少行、有没有错）     │
  ├──────────────────────────────────────────────────────┤
  │ 3. 调度器（Airflow DAG）                             │
  │    - 决定什么时间跑、按什么顺序跑                     │
  │    - 出问题重试、发告警                               │
  └──────────────────────────────────────────────────────┘

  面试官想听的就是这个：不是"我会 PySpark"，而是"我搭过端到端管道"。
""")


# ================================================================
# 第 2 关：准备数据 —— 造一份原始 CSV
# ================================================================
print("===== 第 2 关：造原始数据 =====")

print("""
运行以下代码生成原始数据文件，然后保存为 CSV。
在 Python 终端或新建脚本执行：
""")

make_data_code = '''
import pandas as pd
import random
from datetime import datetime, timedelta

random.seed(42)

# 造 7 天订单数据，故意放一些脏数据
records = []
for day_offset in range(7):
    date = (datetime(2025, 6, 1) + timedelta(days=day_offset)).strftime("%Y-%m-%d")
    for i in range(1000):
        category = random.choice(["电子产品", "服装", "食品", "图书", None])  # None = 脏
        amount = random.choice([
            round(random.uniform(10, 2000), 2),
            -50,     # 脏：负金额
            None,    # 脏：空值
        ])
        user_id = random.randint(1, 500)
        records.append({"date": date, "category": category, "amount": amount, "user_id": user_id})

df = pd.DataFrame(records)
df.to_csv("/Users/thatguy/de-venv/data/raw_orders.csv", index=False)
print(f"生成 {len(df)} 行，\n脏数据: {df['category'].isna().sum()} 空分类, {df['amount'].isna().sum()} 空金额, {(df['amount'] < 0).sum()} 负金额")
print("文件: data/raw_orders.csv")
'''

print(make_data_code)


# ================================================================
# 第 3 关：PySpark 清洗脚本（核心）
# ================================================================
print("===== 第 3 关：PySpark 清洗 + 聚合 =====")

print("""
现在创建文件: ~/de-venv/pipeline_etl.py

这个脚本只做数据处理，不依赖 Airflow——
意味着你可以单独运行它调试，也可以被 Airflow 调。

结构：
  1. load_raw()     → 读 CSV
  2. clean()        → 去脏数据
  3. aggregate()    → 按日期+品类聚合
  4. save_to_mysql() → 写库
  5. main()         → 串起来 + 返回统计信息

===== 你的任务：补全 pipeline_etl.py 的 TODO =====
""")


# ================================================================
# 第 4 关：MySQL 目标表
# ================================================================
print("===== 第 4 关：建目标表 =====")

print("""
在 MySQL 里创建两张表。终端执行：

mysql -u root
""")

create_tables_sql = """
CREATE DATABASE IF NOT EXISTS pipeline_db;
USE pipeline_db;

-- 表 1：每日销售汇总（PySpark 产出）
CREATE TABLE IF NOT EXISTS daily_sales (
    id INT AUTO_INCREMENT PRIMARY KEY,
    report_date DATE NOT NULL,
    category VARCHAR(50) NOT NULL,
    total_amount DECIMAL(12,2),
    order_count INT,
    avg_amount DECIMAL(10,2),
    unique_users INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_cat (report_date, category)
);

-- 表 2：管道运行日志
CREATE TABLE IF NOT EXISTS pipeline_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    run_date DATE NOT NULL,
    dag_run_id VARCHAR(100),
    raw_rows INT COMMENT '原始行数',
    clean_rows INT COMMENT '清洗后行数',
    dirty_count INT COMMENT '脏数据行数',
    start_time DATETIME,
    end_time DATETIME,
    status VARCHAR(20) DEFAULT 'running',
    error_msg TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

print(create_tables_sql)


# ================================================================
# 第 5 关：数据验证
# ================================================================
print("===== 第 5 关：数据验证 =====")

print("""
验证不是"跑完看一下"，而是写死的检查逻辑——不通过就报警。

三条核心验证：
""")

validation_logic = '''
def validate_results(spark, run_date):
    """验证清洗后的数据质量，不通过抛出异常"""
    errors = []

    # 验证 1：行数不能暴增或暴跌（与历史均值比，偏差 > 50% 报警）
    today_count = spark.sql(f"SELECT COUNT(*) FROM daily_sales WHERE report_date = '{run_date}'")
    # 实际项目里查历史 7 天均值对比

    # 验证 2：金额不能有负数
    neg = spark.sql(f"SELECT COUNT(*) FROM daily_sales WHERE report_date = '{run_date}' AND total_amount < 0")
    if neg > 0:
        errors.append(f"发现 {neg} 条负金额")

    # 验证 3：品类不能有空
    null_cat = spark.sql(f"SELECT COUNT(*) FROM daily_sales WHERE report_date = '{run_date}' AND category IS NULL")
    if null_cat > 0:
        errors.append(f"发现 {null_cat} 条空品类")

    if errors:
        raise ValueError("数据验证失败: " + "; ".join(errors))

    print(f"✅ {run_date} 数据验证通过")
'''

print(validation_logic)


# ================================================================
# 第 6 关：Airflow DAG 串起一切
# ================================================================
print("===== 第 6 关：Airflow DAG =====")

print("""
现在创建文件: ~/de-venv/airflow/dags/daily_pipeline.py

这是整条生产线的"指挥官"——
它不处理数据，只决定：
  · 凌晨 2 点跑
  · extract → transform → load → validate 按顺序
  · 哪步失败重试
  · 全失败了告警

===== DAG 结构伪代码 =====
""")

dag_structure = '''
import sys
sys.path.insert(0, "/Users/thatguy/de-venv")

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

# 告警函数（你之前写的）
def alert(context):
    task_id = context["task_instance"].task_id
    error = context["exception"]
    print(f"🚨 {task_id} 失败: {error}")

# 验证函数
def validate_data(**context):
    """调 PySpark 验证结果"""
    # TODO: 连 Spark，查 MySQL，跑验证逻辑
    pass

# DAG 配置
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert,
}

with DAG(
    dag_id="daily_pipeline",
    start_date=datetime(2025, 6, 1),
    schedule="0 2 * * *",      # 每天凌晨 2 点
    catchup=False,
    default_args=default_args,
    tags=["production", "pipeline"],
) as dag:

    # TODO: 创建 4 个任务

    # 1. extract:   BashOperator 调 PySpark 读 CSV
    extract = BashOperator(
        task_id="extract",
        bash_command="python /Users/thatguy/de-venv/pipeline_etl.py --step extract --date {{ ds }}",
    )

    # 2. transform: BashOperator 调 PySpark 清洗 + 聚合
    transform = BashOperator(
        task_id="transform",
        bash_command="python /Users/thatguy/de-venv/pipeline_etl.py --step transform --date {{ ds }}",
    )

    # 3. load: BashOperator 写 MySQL
    load = BashOperator(
        task_id="load",
        bash_command="python /Users/thatguy/de-venv/pipeline_etl.py --step load --date {{ ds }}",
    )

    # 4. validate: PythonOperator 验证数据质量
    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_data,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    extract >> transform >> load >> validate

# {{ ds }} 是 Airflow 的模板变量，自动替换成执行日期，如 "2025-06-01"
'''

print(dag_structure)


# ================================================================
# 第 7 关：填空练习
# ================================================================
print("===== 第 7 关：填空 =====")

print("""
你今天的三个产出文件：
  1. data/raw_orders.csv         — 原始数据
  2. pipeline_etl.py             — PySpark ETL 脚本
  3. airflow/dags/daily_pipeline.py  — Airflow DAG

我提供骨架，你补 TODO。开始写吧。

========================================
做完后在终端验证：

# 1. 生成数据
python -c "
import pandas as pd; import random
random.seed(42)
records = []
from datetime import datetime, timedelta
for d in range(7):
    date = (datetime(2025,6,1)+timedelta(days=d)).strftime('%Y-%m-%d')
    for i in range(1000):
        cat = random.choice(['电子产品','服装','食品','图书',None])
        amt = random.choice([round(random.uniform(10,2000),2), -50, None])
        records.append({'date':date,'category':cat,'amount':amt,'user_id':random.randint(1,500)})
df = pd.DataFrame(records)
df.to_csv('/Users/thatguy/de-venv/data/raw_orders.csv', index=False)
print(f'生成 {len(df)} 行')
"

# 2. 建 MySQL 表
mysql -u root < /Users/thatguy/de-venv/create_tables.sql

# 3. 手动跑 ETL（不通过 Airflow）
python pipeline_etl.py --date 2025-06-01

# 4. 验证 MySQL 里有数据
mysql -u root -e "SELECT report_date, category, total_amount, order_count FROM pipeline_db.daily_sales LIMIT 10"

# 5. DAG 部署
cp daily_pipeline.py ~/de-venv/airflow/dags/
airflow dags unpause daily_pipeline
airflow dags trigger daily_pipeline
""")

print("\n===== Phase 2 Day 10-11 完成 =====")
print("这是面试里你能讲的项目：")
print("  '我用 PySpark + Airflow + MySQL 搭了一条完整的数据管道，'")
print("  '包含脏数据清洗、聚合计算、数据验证、失败重试和告警。'")
print("  '每天凌晨自动跑，处理 X 万行数据。'")
