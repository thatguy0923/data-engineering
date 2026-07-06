#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用户行为日报管道

场景：
  原始 CSV（raw_user_activity.csv）记录用户每天的使用时长和操作次数。
  按"地区"维度汇总，生成日报表写入 MySQL。

数据说明：
  - date:          日期
  - region:        地区（华东/华南/华北/西南）
  - hours_spent:   使用时长（小时）
  - actions:       操作次数
  - user_id:       用户ID

你能用的东西：
  - spark.read.csv()
  - df.filter()
  - F.col().isNotNull()
  - df.groupBy(...).agg(...)
  - F.sum(), F.count(), F.avg(), F.countDistinct()
  - df.withColumnRenamed()
  - df.write.jdbc()
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse
import time

spark = SparkSession.builder \
    .appName("UserActivityPipeline") \
    .master("local[*]") \
    .config("spark.jars", "/Users/thatguy/de-venv/mysql-connector-j.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

MYSQL_URL = "jdbc:mysql://localhost:3306/pipeline_db"
MYSQL_PROPS = {
    "user": "root",
    "password": "",
    "driver": "com.mysql.cj.jdbc.Driver",
}


def load_raw(date_str):
    """加载指定日期的原始数据，返回 (DataFrame, 行数)"""
    df = spark.read.option("header",True).option("inferSchema", True).csv("/Users/thatguy/de-venv/data/raw_user_activity.csv")
    df = df.filter(F.col("date") == date_str)
    count = df.count()
    return df, count


def clean(df):
    """
    清洗：
      - region 不能为空
      - hours_spent 不能为空，且 >= 0
      - actions 不能为空，且 >= 0
    返回 (干净DataFrame, 脏数据行数)
    """
    clean_df = df.filter(
        (F.col("region").isNotNull()) &
        (F.col("hours_spent").isNotNull()) &
        (F.col("hours_spent") >= 0) &
        (F.col("actions").isNotNull()) &
        (F.col("actions") >= 0)
    )
    clean_count = clean_df.count()
    before = df.count()
    dirty = before - clean_count
    return clean_df, dirty

def aggregate(clean_df):
    """
    按 (date, region) 聚合，计算：
      - total_hours:   总使用时长
      - total_actions: 总操作次数
      - avg_hours:     人均使用时长
      - active_users:  独立用户数
    重命名 date → report_date
    """
    report_df = clean_df.groupBy("date", "region").agg(
        F.sum("hours_spent").alias("total_hours"),
        F.sum("actions").alias("total_actions"),
        F.avg("hours_spent").alias("avg_hours"),
        F.countDistinct("user_id").alias("active_users")
    )
    report_df = report_df.withColumnRenamed("date", "report_date")
    report_df.show(10)
    return report_df


def save_to_mysql(agg_df):
    """将聚合结果写入 daily_user_report 表 (append 模式)"""
    agg_df.write.jdbc(
        url = MYSQL_URL,
        table = "daily_user_report",
        mode = "append",
        properties = MYSQL_PROPS
    )
    pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    start = time.time()
    date_str = args.date
    print(f"\n===== 用户行为管道: {date_str} =====")

    df, raw_count = load_raw(date_str)
    clean_df, dirty_count = clean(df)
    agg_df = aggregate(clean_df)
    save_to_mysql(agg_df)

    elapsed = time.time() - start
    print(f"\n✅ 完成: {elapsed:.2f} 秒 | 原始{raw_count}行 → 干净{raw_count - dirty_count}行")


if __name__ == "__main__":
    main()
