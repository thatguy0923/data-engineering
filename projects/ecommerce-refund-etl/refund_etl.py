from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from refund_config import Config, logger, PipelineError
import argparse, time
from pyspark.sql.functions import rand

spark = SparkSession.builder.appName("RefundETL").master("local[*]").config("spark.jars", "/Users/thatguy/de-venv/mysql-connector-j.jar").getOrCreate()


def load_raw(date_str):

    df = spark.read.option("header", True).option("inferSchema", True).csv("/Users/thatguy/de-venv/data/refund_orders.csv")
    df = df.filter(F.col("date") == date_str)
    count = df.count()
    logger.info(f"行数为: {count}")

    return df, count


def clean(df):

    filted = df.filter(
        (F.col("amount").isNotNull()) &
        (F.col("refund_amount").isNotNull()) &
        (F.col("refund_amount") >= 0) &
        (F.col("refund_reason").isNotNull())
    )
    before_count = filted.count()
    clean_df = filted.dropDuplicates(["order_id"])
    duplicates = before_count - clean_df.count()
    dirty = {
    "null_amount": df.filter(F.col("amount").isNull()).count(),
    "null_refund": df.filter(F.col("refund_amount").isNull()).count(),
    "negative_refund": df.filter(F.col("refund_amount") < 0).count(),
    "null_reason": df.filter(F.col("refund_reason").isNull()).count(),
    "duplicate_count": duplicates,
    }

    return clean_df, dirty



def diagnose_skew(clean_df):

    grouped = clean_df.groupBy("store_id").count().orderBy(F.col("count").desc())
    grouped.show(5)
    result = {i["store_id"]: i["count"] for i in grouped.collect()}
    logger.info(f"共 {len(result)} 个key,最大：{max(result.values())} 行")
    
    return result


def salt_and_aggregate(clean_df, date_str):
    store_count = clean_df.groupBy("store_id").count()
    salted_df = clean_df.join(store_count, on = "store_id")
    salted_df = salted_df.withColumn("salted_key", 
        F.when(F.col("count") > Config.SKEW_THRESHOLD, F.concat(F.col("store_id"), F.lit("_"), (F.rand() * Config.SALT_COUNT)))
        .otherwise(F.col("store_id").cast("string"))
    )
    step2 = salted_df.groupBy("salted_key", "category").agg(
        F.count("*").alias("total_orders"),
        F.sum("refund_amount").alias("total_refund"),
        F.countDistinct("user_id").alias("distinct_users"),
        F.sum("amount").alias("total_amount"),
    )
    step3 = step2.withColumn("key",
        F.when(F.col("salted_key").contains("_"), F.split(F.col("salted_key"), "_")[0])
        .otherwise(F.col("salted_key"))
    )
    agg_df = step3.groupBy("key", "category").agg(
        F.sum("total_orders").alias("total_orders"),
        F.sum("total_refund").alias("total_refund"),
        F.sum("distinct_users").alias("distinct_users"),
        F.sum("total_amount").alias("total_amount"),
        (F.sum("total_refund") / F.sum("total_orders")).alias("avg_refund_amount"),
        (F.sum("total_refund") / F.sum("total_amount")).alias("refund_rate"),
    )
    
    return agg_df

def save_to_mysql(agg_df, date_str):

    agg_df = agg_df.withColumnRenamed("key", "store_id").withColumnRenamed("category", "product_category").withColumn("report_date", F.lit(date_str)).drop("total_amount")
    agg_df.write.jdbc(
        url = Config.MYSQL_URL,
        table = "daily_refund_report",
        mode = "append",
        properties = Config.MYSQL_PROPS,
    )

    return


def validate_results(date_str):

    df = spark.read.jdbc(
        url = Config.MYSQL_URL,
        table = "daily_refund_report",
        properties = Config.MYSQL_PROPS,
    )
    errors = []
    if df.count() == 0:
        errors.append(f"表格为空")
    neg = df.filter(F.col("total_refund") < 0).count()
    if neg > 0:
        errors.append("存在负数refund值")
    cat = df.filter(F.col("product_category").isNull()).count()
    if cat > 0:
        errors.append("存在空品类")
    if errors:
        raise PipelineError(f"验证失败：" + "; ".join(errors))
    
    return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="处理日期 YYYY-MM-DD")
    parser.add_argument("--step", choices=["extract", "transform", "load", "validate", "all"],
                        default="all", help="指定跑哪一步")
    args = parser.parse_args()
    start = time.time()
    date_str = args.date
    if args.step in ("extract", "all"):
        df, raw_count = load_raw(date_str)
    if args.step in ("transform", "all"):
        clean_df, dirty = clean(df)
        agg_df = salt_and_aggregate(clean_df, date_str)
    if args.step in ("load", "all"):
        save_to_mysql(agg_df, date_str)
    if args.step in ("validate", "all"):
        validate_results(date_str)
    elapsed = time.time() - start
    print(f"\n 管道完成！运行时间：{elapsed:.2f} 秒")

if __name__ == "__main__":
    main()