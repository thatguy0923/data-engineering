#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================
综合大练习：电商退款分析系统
===============================================================

场景：
  你是某电商的数据工程师。公司需要每天分析退款订单：
  - 哪些品类退款最多？
  - 哪些店铺退款率异常？
  - 数据倾斜（一家大店占 40% 的退款）

你要从 0 到 1 搭完整条生产线。

===============================================================
涉及知识点（全部）：
===============================================================

Phase 1:
  [ ] pandas 造数据 + 基础统计
  [ ] SQLAlchemy (或直接 mysql CLI) 建表
  [ ] argparse 命令行参数
  [ ] logging 日志（替代 print）
  [ ] 自定义异常 + 配置类

Phase 2:
  [ ] PySpark 读 CSV / filter / select / groupBy / agg / withColumn
  [ ] 分区观察 (getNumPartitions / repartition)
  [ ] 窄依赖 vs 宽依赖（标注哪些操作会触发 Shuffle）
  [ ] 数据倾斜 + 加盐法
  [ ] write.jdbc 写 MySQL
  [ ] 数据验证（读回 MySQL 检查）
  [ ] Airflow DAG + 重试 + 告警

===============================================================
产出文件（共 4 个）：
===============================================================

  1. data/generate_refund_data.py    — pandas 造数据
  2. refund_etl.py                   — PySpark ETL 核心脚本
  3. refund_config.py                — 配置类 + 日志 + 自定义异常
  4. airflow/dags/refund_pipeline.py — Airflow DAG

===============================================================
业务需求
===============================================================

输入: refund_orders.csv (7 天×2000 行 = 14000 行)
  - order_id:      订单号
  - user_id:       用户 ID
  - product_category: 品类（电子/服装/食品/图书/美妆）
  - store_id:      店铺 ID（1~50，store_1 是"鲸鱼店"占 40%）
  - amount:        原订单金额
  - refund_amount: 退款金额
  - refund_reason: 退款原因（质量问题/不喜欢/发错货/未收到/NULL）
  - order_date:    订单日期（2025-06-01 ~ 2025-06-07）

脏数据特征:
  - refund_reason 有 NULL
  - refund_amount 有负数和 NULL
  - amount 有 NULL
  - 少量重复 order_id

按 (order_date, store_id, product_category) 聚合:
  - total_orders:      订单总数
  - total_refund:      退款总金额
  - refund_rate:       退款率（退款金额 / 金额）
  - distinct_users:    涉及用户数
  - avg_refund_amount: 平均退款金额

写入 MySQL 表: refund_db.daily_refund_report

===============================================================
评分标准
===============================================================

  [ ] Part 1: pandas 造数据 + 基础统计       （20 分）
  [ ] Part 2: PySpark ETL（读→洗→聚→写）   （30 分）
  [ ] Part 3: 数据倾斜诊断 + 加盐法           （20 分）
  [ ] Part 4: 数据验证                        （10 分）
  [ ] Part 5: 配置类 + 日志 + 异常             （10 分）
  [ ] Part 6: Airflow DAG                      （10 分）

===============================================================
开始
===============================================================
"""

# ╔══════════════════════════════════════════════════════════════╗
# ║  Part 1: pandas 造数据 + 基础统计                          ║
# ╚══════════════════════════════════════════════════════════════╝

print("""
===== Part 1: pandas =====

创建文件: data/generate_refund_data.py

任务:
  1. 用 pandas 造 14000 行退款数据（7 天 × 2000 行）
  2. store_1 占 40%，其他 49 家店瓜分 60%（造数据倾斜）
  3. 故意放脏数据：NULL / 负数 / 空值
  4. 用 DataFrame 算 3 个基础统计（用 pandas 的 groupby + agg）:
     a) 每天多少行？
     b) 每个品类的退款金额总和？
     c) 鲸鱼店（store_1）占多少行？
  5. 保存为 data/refund_orders.csv

提示:
  import pandas as pd
  import random; random.seed(42)

  store_id: 40% 概率选 1，60% 概率选 2~50
  refund_amount: 20% 概率脏（None 或负数）
  refund_reason: 20% 概率 None
  amount: 5% 概率 None

  计算占比: store_1_count / total_count * 100

写完告诉我，我检查。
""")


# ╔══════════════════════════════════════════════════════════════╗
# ║  Part 2: 建 MySQL 表                                       ║
# ╚══════════════════════════════════════════════════════════════╝

print("""
===== Part 2: MySQL 建表 =====

终端执行:

mysql -u root -e "
CREATE DATABASE IF NOT EXISTS refund_db;
USE refund_db;

CREATE TABLE IF NOT EXISTS daily_refund_report (
    id INT AUTO_INCREMENT PRIMARY KEY,
    report_date DATE NOT NULL,
    store_id INT NOT NULL,
    product_category VARCHAR(20) NOT NULL,
    total_orders INT,
    total_refund DECIMAL(12,2),
    refund_rate DECIMAL(6,4),
    distinct_users INT,
    avg_refund_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_store_cat (report_date, store_id, product_category)
);
"
""")


# ╔══════════════════════════════════════════════════════════════╗
# ║  Part 3: refund_config.py                                  ║
# ╚══════════════════════════════════════════════════════════════╝

print("""
===== Part 3: 配置类 + 日志 + 自定义异常 =====

创建文件: refund_config.py

任务:
  1. 定义一个 Config 类，包含:
     - CSV_PATH = "/Users/thatguy/de-venv/data/refund_orders.csv"
     - MYSQL_URL = "jdbc:mysql://localhost:3306/refund_db"
     - MYSQL_PROPS = {"user": "root", "password": "", "driver": "com.mysql.cj.jdbc.Driver"}
     - SALT_COUNT = 5
     - SKEW_THRESHOLD = 500  (超过此行数的 key 视为倾斜)

  2. 设置 logging:
     - 格式: "[时间] [级别] 消息"
     - 同时输出到控制台和文件 refund_etl.log

  3. 自定义异常类 PipelineError(Exception)

提示:
  import logging
  logging.basicConfig(
      level=logging.INFO,
      format="%(asctime)s [%(levelname)s] %(message)s",
      handlers=[logging.FileHandler("refund_etl.log"), logging.StreamHandler()]
  )
  logger = logging.getLogger(__name__)
""")


# ╔══════════════════════════════════════════════════════════════╗
# ║  Part 4: refund_etl.py 核心 ETL（重点）                    ║
# ╚══════════════════════════════════════════════════════════════╝

print("""
===== Part 4: PySpark ETL 核心 =====

创建文件: refund_etl.py

你要从头写一个完整的 ETL 脚本，包含以下函数。

==========================================
骨架
==========================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from refund_config import Config, logger, PipelineError
import argparse, time

spark = SparkSession.builder
    .appName("RefundETL")
    .master("local[*]")
    .config("spark.jars", "/Users/thatguy/de-venv/mysql-connector-j.jar")
    .getOrCreate()

==========================================
你需要写的函数:
==========================================

1) load_raw(date_str) → (DataFrame, row_count)
   - 读 CSV（header + inferSchema）
   - filter date == date_str
   - logger.info() 记录行数

2) clean(df) → (clean_df, dirty_stats_dict)
   - 过滤: amount NOT NULL, refund_amount NOT NULL AND >= 0, refund_reason NOT NULL
   - 去重: 按 order_id 去重（dropDuplicates）
   - 返回清洗后 DataFrame + 脏数据统计字典
     {"null_amount": N, "null_refund": N, "negative_refund": N, ...}
   - 注释标注：哪些操作是窄依赖、哪些是宽依赖

3) diagnose_skew(clean_df) → dict
   - groupBy store_id 统计行数，按 count 降序排列
   - 返回 {store_id: count} 的 dict（前 5 名）
   - logger.info() 输出倾斜情况

4) salt_and_aggregate(clean_df, date_str) → agg_df
   - 先按 store_id 统计行数
   - 行数 > SKEW_THRESHOLD 的 key 加盐（concat + rand）
   - 两步聚合（salted_key → 去盐 → 合并）
   - 计算: total_orders, total_refund, distinct_users, avg_refund_amount
   - 计算 refund_rate = total_refund / 该 store+品类 对应的原始总金额

   注意：这里需要处理 refund_rate 的计算——你需要在清洗后保存
         original_total_amount (= sum(amount))，和 refund 结果一起
         计算。提示：可以做两次 groupBy，一次算 amount 总和，
         一次算 refund 总和，然后 join。

5) save_to_mysql(agg_df, date_str)
   - write.jdbc 写入 refund_db.daily_refund_report
   - mode="append"

6) validate_results(date_str)
   - spark.read.jdbc 读回 MySQL
   - 检查: 行数 > 0、无负 refund、无空品类
   - 不通过抛 PipelineError

7) main()
   - argparse: --date (必需), --step (可选)
   - 流程串联
   - 计时 + logger.info()

==========================================
注意:
  - 全程用 logger.info() 替代 print()
  - narrow/wide 依赖用注释标注
  - 退款率 = 退款金额 / 原订单金额，不是一个 agg 能算出来的
    需要保留 amount 和 refund_amount 两个字段分别处理
""")


# ╔══════════════════════════════════════════════════════════════╗
# ║  Part 5: Airflow DAG                                       ║
# ╚══════════════════════════════════════════════════════════════╝

print("""
===== Part 5: Airflow DAG =====

创建文件: airflow/dags/refund_pipeline.py

任务:
  1. 一个 PythonOperator 调 refund_etl.py --step all --date <日期>
  2. default_args: retries=2, retry_delay=5分钟
  3. on_failure_callback 告警函数
  4. schedule="0 3 * * *"（每天凌晨 3 点）

提示:
  参考 daily_pipeline.py 的 PythonOperator + subprocess.run() 模式
  date 用 context["dag_run"].logical_date，手动触发兜底 date.today()
""")


# ╔══════════════════════════════════════════════════════════════╗
# ║  开始                                                        ║
# ╚══════════════════════════════════════════════════════════════╝

print("""
===============================================================
从 Part 1 开始，做完一个告诉我，再开始下一个。
===============================================================
""")
