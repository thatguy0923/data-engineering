#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整管道 ETL 脚本 —— PySpark 清洗 + 聚合 + 写 MySQL

用法：
  python pipeline_etl.py --date 2025-06-01
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse
import time

spark = SparkSession.builder \
    .appName("DailyPipeline") \
    .master("local[*]") \
    .config("spark.jars", "/Users/thatguy/de-venv/mysql-connector-j.jar") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ================================================================
# Part 1: 加载原始 CSV
# ================================================================
def load_raw(date_str):
    """从 CSV 加载指定日期的原始订单数据"""
    print(f"📥 加载 {date_str} 的原始数据...")

    # TODO 1: 用 spark.read 读取 CSV 文件
    # 文件路径：/Users/thatguy/de-venv/data/raw_orders.csv
    # 提示：.option("header", True).option("inferSchema", True)
    # 你的代码：
    df = spark.read.option("header", True).option("inferSchema", True).csv("/Users/thatguy/de-venv/data/raw_orders.csv")

    # TODO 2: 筛选出 date == date_str 的行
    # 提示：df.filter(F.col("date") == date_str)
    # 你的代码：
    df = df.filter(F.col("date") == date_str)
    
    raw_count = df.count()
    print(f"   原始行数: {raw_count}")
    return df, raw_count


# ================================================================
# Part 2: 清洗
# ================================================================
def clean(df):
    """清洗：去掉 category 为空、amount 为空或负数的行"""
    print("🧹 清洗数据...")
    before = df.count()

    # TODO 3: 过滤掉脏数据
    # 三个条件，用 & 连接：
    #   category IS NOT NULL
    #   amount IS NOT NULL
    #   amount >= 0
    # 提示：F.col("列名").isNotNull(),  &  两边各自加括号
    # 你的代码：
    clean_df = df.filter((F.col("category").isNotNull()) & (F.col("amount").isNotNull()) & (F.col("amount") >= 0))
    

    after = clean_df.count()
    dirty = before - after
    print(f"   洗前行数: {before}")
    print(f"   洗后行数: {after}")
    print(f"   脏数据:   {dirty} 行")
    return clean_df, dirty


# ================================================================
# Part 3: 聚合计算
# ================================================================
def aggregate(clean_df):
    """按 (date, category) 聚合，计算核心指标"""
    print("📊 聚合计算...")

    # TODO 4: 按 date + category 做 groupBy 聚合
    # 需要四个指标：
    #   total_amount = sum(amount)
    #   order_count  = count(*)
    #   avg_amount   = avg(amount)
    #   unique_users = countDistinct(user_id)
    # 提示：clean_df.groupBy("date", "category").agg(...)
    # 你的代码：
    result = clean_df.groupBy("date", "category").agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("order_count"),
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("user_id").alias("unique_users")
    )

    # TODO 5: 把 date 列重命名为 report_date（对应 MySQL 表列名）
    # 提示：result.withColumnRenamed("旧名", "新名")
    # 你的代码：
    result = result.withColumnRenamed("date", "report_date")

    print(f"   聚合后行数: {result.count()}")
    result.show(10)
    return result


# ================================================================
# MySQL 连接配置（模块级别，多处复用）
# ================================================================
MYSQL_URL = "jdbc:mysql://localhost:3306/pipeline_db"
MYSQL_PROPS = {
    "user": "root",
    "password": "",
    "driver": "com.mysql.cj.jdbc.Driver",
}

# ================================================================
# Part 4: 写 MySQL
# ================================================================
def save_to_mysql(agg_df, date_str):
    """将聚合结果写入 MySQL 的 daily_sales 表"""
    print(f"💾 写入 MySQL: {date_str}")

    # TODO 6: 用 write.jdbc() 写入 MySQL
    # 提示：agg_df.write.jdbc(url=..., table="daily_sales", mode="append", properties=...)
    # 你的代码：
    agg_df.write.jdbc(
        url=MYSQL_URL,
        table="daily_sales",
        mode="append",
        properties=MYSQL_PROPS
    )

    print("   ✅ 写入完成")


# ================================================================
# Part 5: 数据验证
# ================================================================
def validate_results(date_str):
    """检查写入 MySQL 的数据质量"""
    print(f"🔍 验证 {date_str} 数据...")

    # 读回刚才写入的数据
    df = spark.read.jdbc(
        url=MYSQL_URL, table="daily_sales", properties=MYSQL_PROPS
    ).filter(F.col("report_date") == date_str)

    errors = []

    if df.count() == 0:
        errors.append("没有数据写入")

    neg = df.filter(F.col("total_amount") < 0).count()
    if neg > 0:
        errors.append(f"发现 {neg} 条负金额")

    null_cat = df.filter(F.col("category").isNull()).count()
    if null_cat > 0:
        errors.append(f"发现 {null_cat} 条空品类")

    if errors:
        raise ValueError("验证失败: " + "; ".join(errors))

    print("   ✅ 数据验证通过")


# ================================================================
# Part 6: 主流程
# ================================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="处理日期 YYYY-MM-DD")
    parser.add_argument("--step", choices=["extract", "transform", "load", "validate", "all"],
                        default="all", help="指定跑哪一步")
    args = parser.parse_args()

    start = time.time()
    date_str = args.date
    print(f"\n===== 管道开始: {date_str} (step={args.step}) =====")

    if args.step in ("extract", "all"):
        df, raw_count = load_raw(date_str)

    if args.step in ("transform", "all"):
        clean_df, dirty_count = clean(df)
        agg_df = aggregate(clean_df)

    if args.step in ("load", "all"):
        save_to_mysql(agg_df, date_str)

    if args.step in ("validate", "all"):
        validate_results(date_str)

    elapsed = time.time() - start
    print(f"\n✅ 管道完成: {elapsed:.2f} 秒")


if __name__ == "__main__":
    main()
