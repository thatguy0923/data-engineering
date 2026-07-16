"""Tatoeba 语料清洗 + 质量校验（PySpark + Great Expectations）。

raw → 去重 → 长度过滤(5-80) → NFKC 规范化 → 语言检测兜底 → GX 校验 → cleaned 层。
运行需将 PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON 指向虚拟环境 Python。
"""
import unicodedata

import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValueLengthsToBeBetween,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

RAW = "data/raw/tatoeba_raw.parquet"
CLEANED = "data/cleaned/tatoeba_clean.parquet"


def to_halfwidth(s):
    """NFKC 规范化：全角字母/数字/符号统一为半角。"""
    return None if s is None else unicodedata.normalize("NFKC", s)


def detect_lang(s):
    """按 Unicode 范围检测语种：含假名→jpn，含谚文→kor，否则 other。"""
    if s is None:
        return "other"
    if any('぀' <= c <= 'ヿ' for c in s):
        return "jpn"
    if any('가' <= c <= '힣' for c in s):
        return "kor"
    return "other"


def clean(df):
    """去重 → 长度过滤 → NFKC → 语言检测兜底（丢弃标签与内容不符的行）。"""
    norm_udf = F.udf(to_halfwidth, StringType())
    detect_udf = F.udf(detect_lang, StringType())
    df = df.dropDuplicates(["text"])
    df = df.filter((F.length("text") >= 5) & (F.length("text") <= 80))
    df = df.withColumn("text", norm_udf(F.col("text")))
    df = df.withColumn("detected", detect_udf(F.col("text")))
    df = df.filter(F.col("detected") == F.col("lang"))
    return df.select("id", "lang", "text")


def validate(df_pd):
    """GX 校验：text 非空、lang ∈ {jpn,kor}、长度 5-80。返回是否通过。"""
    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_pandas(name="pandas_ds")
    asset = ds.add_dataframe_asset(name="pandas_asset")
    bd = asset.add_batch_definition_whole_dataframe(name="whole_dataframe")
    suite = context.suites.add(gx.ExpectationSuite(name="suite"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="text"))
    suite.add_expectation(ExpectColumnValuesToBeInSet(column="lang", value_set=["jpn", "kor"]))
    suite.add_expectation(ExpectColumnValueLengthsToBeBetween(column="text", min_value=5, max_value=80))
    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name="vd", data=bd, suite=suite))
    return vd.run(batch_parameters={"dataframe": df_pd}).success


def main():
    spark = SparkSession.builder.appName("tatoeba_clean").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df_clean = clean(spark.read.parquet(RAW)).cache()
    print(f"清洗后行数: {df_clean.count()}")
    df_clean.groupBy("lang").count().show()

    if validate(df_clean.toPandas()):
        df_clean.write.mode("overwrite").parquet(CLEANED)
        print(f"GX 通过，已落地 {CLEANED}")
    else:
        print("GX 校验失败，不落地")
    spark.stop()


if __name__ == "__main__":
    main()
