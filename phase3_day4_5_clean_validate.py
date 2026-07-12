"""
============================================================
Phase 3 · Day 4-5  PySpark 清洗 + 语言检测 + GX 质量校验
============================================================
目标：把 Day 3 落地的 26 万句脏语料，用 PySpark 洗成干净语料，
      再用 Great Expectations 校验质量，落地到 cleaned 层。
这是 Phase 3 最"数据工程"的一课，【大量复用 Phase 2 的 PySpark 和 GX】。

【输入】 data/raw/tatoeba_raw.parquet   （Day 3 落地，26 万句，raw 层）
【输出】 data/cleaned/tatoeba_clean.parquet （清洗+校验后，cleaned 层）

【要洗的脏点（你 Day 3 体检记下的清单）】
  1. 重复例句            → 去重
  2. 长度两头异常        → 过滤太短(没信息)/太长(整段话)的
  3. 全角字符(６月→618)  → NFKC 规范化统一成半角
  4. 语种没验证          → 语言检测兜底，丢掉"挂羊头卖狗肉"的行
                          （文件说 jpn 但其实是英文/纯汉字的，踢掉）

【为什么用 PySpark 不用 pandas？】
  - 26 万句 pandas 也能跑，但这是"练真功夫"：真实语料可能几千万上亿句
  - 复用你学的 DataFrame API / UDF / 分布式思维，简历讲得通
  - 面试权衡话术：小数据 pandas 够用，大数据/要并行才上 Spark

【运行方式】
  cd ~/de-venv && source bin/activate
  python phase3_day4_5_clean_validate.py
  ⚠️ 确保 PYSPARK_PYTHON 指向 venv 的 python（你之前配过 ~/.zshrc）

【本节规则】
  每个 TODO 自己写，卡住看提示。分两天：
    Day 4 = TODO 1-6（PySpark 清洗）
    Day 5 = TODO 7-8（GX 校验 + 落地 + 理解题）
============================================================
"""

import unicodedata
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ============================================================
# 第 0 步（已给你，不用改）：建 SparkSession
# ============================================================
spark = SparkSession.builder \
    .appName("tatoeba_clean") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")   # 少打点日志，输出清爽
print("SparkSession 就绪 ✅\n")


# ============================================================
# ===================  Day 4：PySpark 清洗  ==================
# ============================================================

# ============================================================
# TODO 1【读入 raw + 初步探查】
# ------------------------------------------------------------
# 要求：
#   1) 用 spark.read.parquet("data/raw/tatoeba_raw.parquet") 读进 df
#   2) df.printSchema()          看列和类型
#   3) df.count()                看总行数（应约 26 万）
#   4) df.show(5, truncate=False)  看前 5 行（truncate=False 不截断长文本）
#   5) 按 lang 分组计数：df.groupBy("lang").count().show()
#
# 提示：
#   - Spark 读 parquet 会自动带上 Day 3 存的列名 id/lang/text，不用再指定
# 你的答案：
df = spark.read.parquet("data/raw/tatoeba_raw.parquet")
df.printSchema()
print(f"总行数: {df.count()}")
df.show(5, truncate=False)
df.groupBy("lang").count().show()



# ============================================================
# TODO 2【去重】
# ------------------------------------------------------------
# 要求：
#   1) 按 text 去重：df.dropDuplicates(["text"])，存进 df2
#   2) 打印去重前后行数，看掉了多少
#
# 想一想：为什么按 "text" 去重，而不是按 "id"？
#   （id 是 Tatoeba 的句子编号，天然唯一；我们要去的是"内容重复"的例句）
# 你的答案：
df2 = df.dropDuplicates(["text"])
print(f"去重前行数: {df.count()}")
print(f"去重后行数: {df2.count()}")



# ============================================================
# TODO 3【长度过滤】
# ------------------------------------------------------------
# 目标：丢掉太短（没学习价值）和太长（整段话，不是例句）的。
# 要求：
#   1) 用 F.length("text") 造一个长度，过滤保留 5 <= 长度 <= 80 的行
#         df3 = df2.filter((F.length("text") >= 5) & (F.length("text") <= 80))
#   2) 打印过滤前后行数
#
# 提示：
#   - Spark 里多条件要用 & 连接，每个条件【必须加括号】：(cond1) & (cond2)
#   - 阈值 5/80 是拍脑袋定的，真实项目会看长度分布再定，先这么过
# 你的答案：
df3 = df2.filter((F.length("text") >= 5) & (F.length("text") <= 80))
print(f"过滤前行数: {df2.count()}")
print(f"过滤后行数: {df3.count()}")



# ============================================================
# TODO 4【全角 → 半角规范化（UDF + NFKC）】★本课核心技术点
# ------------------------------------------------------------
# Day 3 发现 "６月１８日" 用了全角数字，要统一成半角 "618"。
# 标准做法：Unicode NFKC 规范化 —— 一行 unicodedata.normalize 搞定，
# 它能把全角字母/数字/符号统一成半角（还能合并一些等价字符）。
#
# 但 Spark 内置函数没有 NFKC，得自己写【UDF（用户自定义函数）】：
#
# 要求：
#   1) 写一个普通 python 函数：
#         def to_halfwidth(s):
#             if s is None: return None
#             return unicodedata.normalize("NFKC", s)
#   2) 注册成 Spark UDF：
#         norm_udf = F.udf(to_halfwidth, StringType())
#   3) 用它新增/覆盖 text 列：
#         df4 = df3.withColumn("text", norm_udf(F.col("text")))
#   4) 抽几行验证：找含全角数字的看有没有变半角
#         df4.filter(F.col("text").rlike("[0-9]")).show(5, truncate=False)
#
# 提示：
#   - UDF = 把普通 python 函数包装成能在 Spark 列上运行的函数
#   - unicodedata 是标准库，Spark worker 能直接用，不用装
#   - UDF 比内置函数慢（要在 python worker 里逐行跑），能用内置就别 UDF；
#     但 NFKC 内置没有，这里 UDF 是合理的
# 你的答案：
def to_halfwidth(s):
    if s is None:
        return None
    return unicodedata.normalize("NFKC", s)
norm_udf = F.udf(to_halfwidth, StringType())
df4 = df3.withColumn("text", norm_udf(F.col("text")))
df4.filter(F.col("text").rlike("[0-9]")).show(5, truncate = False)

# ============================================================
# TODO 5【语言检测兜底（UDF + Unicode 范围）】★核心
# ------------------------------------------------------------
# 文件名说 jpn 就一定是日语吗？不一定——可能混进纯英文、纯汉字的行。
# 我们做个【轻量语言检测】：靠 Unicode 字符范围判断，比通用分类器在短文本上更准。
#   - 日语特征：含【平假名 ぀-ゟ】或【片假名 ゠-ヿ】
#   - 韩语特征：含【谚文 가-힣】
#   （只有汉字不算日语——汉字日中都有，会混；必须有假名才判日语）
#
# 要求：
#   1) 写函数 detect_lang(s) 返回 "jpn" / "kor" / "other"：
#         - 若含平假名或片假名 → "jpn"
#         - 若含谚文           → "kor"
#         - 否则               → "other"
#      提示：用 any() + 判断每个字符的 Unicode 码点范围，例如
#         has_kana = any('぀' <= c <= 'ヿ' for c in s)
#         has_hangul = any('가' <= c <= '힣' for c in s)
#   2) 注册 UDF，新增一列 detected：
#         detect_udf = F.udf(detect_lang, StringType())
#         df5 = df4.withColumn("detected", detect_udf(F.col("text")))
#   3) 只保留 detected 和 lang 一致的行（真的是它自称的语种）：
#         df5 = df5.filter(F.col("detected") == F.col("lang"))
#   4) 打印：过滤掉了多少行？剩下的 lang 分布？
#         被踢掉的看几条：df4...  （可选，看看混进来的是啥）
#
# 提示：
#   - 这一步是"数据质量兜底"，把标签和内容对不上的脏行清掉
#   - 面试点：为什么用 Unicode 范围而不是 langdetect 库？
#     → 短例句上范围判断更稳、零依赖、够快；通用分类器对短文本易误判
# 你的答案：
def detect_lang(s):
    if s is None:
        return "other"
    has_kana = any('぀' <= c <= 'ヿ' for c in s)
    has_hangul = any('가' <= c <= '힣' for c in s)
    if has_kana:
        return "jpn"
    elif has_hangul:
        return "kor"
    else:
        return "other"
detect_udf = F.udf(detect_lang, StringType())
df5 = df4.withColumn("detected", detect_udf(F.col("text")))
df5_filtered = df5.filter(F.col("detected") == F.col("lang"))
print(f"过滤掉的行数: {df5.count() - df5_filtered.count()}")
df5_filtered.groupBy("lang").count().show()


# ============================================================
# TODO 6【清洗小结：看看洗剩多少】
# ------------------------------------------------------------
# 要求：
#   1) 把最终 df5 缓存一下（后面 GX 还要用）：df_clean = df5.select("id","lang","text").cache()
#   2) 打印最终行数、lang 分布
#   3) df_clean.show(10, truncate=False) 肉眼扫一遍，是不是干净了
#
# 对比一下：raw 26 万 → 现在剩多少？掉了多少百分比？心里有个数。
# 你的答案：
df_clean = df5_filtered.select("id", "lang", "text").cache()
print(f"最终行数: {df_clean.count()}")
df_clean.groupBy("lang").count().show()
df_clean.show(10, truncate=False)



# ============================================================
# ================  Day 5：GX 校验 + 落地  ===================
# ============================================================

# ============================================================
# TODO 7【Great Expectations 质量校验】★复用 Day 12
# ------------------------------------------------------------
# 清洗完不能盲信"我洗干净了"，要用 GX 拿规则【自动验证】。
# 数据量不大，这里把 Spark df 转成 pandas 用 GX 的 pandas 引擎（最简单）。
#   df_pd = df_clean.toPandas()
# （生产大数据才用 GX spark 引擎；这里能装内存就 toPandas，是合理权衡）
#
# 要求（回忆 Day 12 的四层：Context→DataSource/Asset/BatchDefinition→Suite→ValidationDefinition）：
#   1) import great_expectations as gx；context = gx.get_context(mode="ephemeral")
#   2) 建 pandas 数据源 + dataframe asset + whole_dataframe 的 batch definition
#   3) 建 Suite，加这几条期望（Expectation）：
#         - text 不能为空        expect_column_values_to_not_be_null("text")
#         - lang 只能是 jpn/kor  expect_column_values_to_be_in_set("lang", ["jpn","kor"])
#         - text 长度在范围内    expect_column_value_lengths_to_be_between("text", min_value=5, max_value=80)
#   4) 建 ValidationDefinition，run(batch_parameters={"dataframe": df_pd})
#   5) 打印 result.success（True 才算过）
#
# 提示：
#   - 具体 API 忘了就翻你 phase2_day12_great_expectations.py，结构一模一样
#   - 如果某条期望没过，说明清洗没做干净——回头看是哪步漏了（这正是 GX 的价值）
# 你的答案：
import great_expectations as gx
context = gx.get_context(mode = "ephemeral")
ds = context.data_sources.add_pandas(name = "pandas_ds")
asset = ds.add_dataframe_asset(name = "pandas_asset")
bd = asset.add_batch_definition_whole_dataframe(name = "whole_dataframe")

from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValueLengthsToBeBetween,
)
suite = context.suites.add(gx.ExpectationSuite(name = "suite"))
suite.add_expectation(ExpectColumnValuesToNotBeNull(column = "text"))
suite.add_expectation(ExpectColumnValuesToBeInSet(column = "lang", value_set = ["jpn", "kor"]))
suite.add_expectation(ExpectColumnValueLengthsToBeBetween(column = "text", min_value = 5, max_value = 80))

vd = context.validation_definitions.add(
    gx.ValidationDefinition(name = "vd", data = bd, suite = suite)
)
df_pd = df_clean.toPandas()
result = vd.run(batch_parameters = {"dataframe": df_pd})
print(f"GX 校验结果: {result.success}")

# ============================================================
# TODO 8【落地 cleaned 层 + 理解题】
# ------------------------------------------------------------
# 只有 GX 校验通过（result.success==True）才落地，不然别存脏数据。
# 要求：
#   1) if result.success:  用 Spark 把 df_clean 写到 data/cleaned/
#         df_clean.write.mode("overwrite").parquet("data/cleaned/tatoeba_clean.parquet")
#      else: print 校验失败，不落地
#   2) 打印落地成功
#
# 理解题（写注释）：
#   8-1) 为什么"语言检测"用 Unicode 范围而不是通用语言分类库？
#   8-2) UDF 和 Spark 内置函数比，什么时候该用 UDF？为什么能不用就不用？
#   8-3) 这一步产出的 data/cleaned/，下一步（Day 6-7）拿来干嘛？
#        （提示：向量化 → 存 Milvus → 语义检索）
#
# 你的答案（写注释）：
#   8-1)因为可以直接检测字符编码，不需要额外引入库
#   8-2)当spark内置函数满足不了需求的时候用udf，因为udf的速度会比python的函数慢得多
#   8-3)向量化数据存入向量库，之后在进行语义检索等应用操作
if result.success:
    df_clean.write.mode("overwrite").parquet("data/cleaned/tatoeba_clean.parquet")
    print("落地成功 ✅")
else:
    print("GX 校验失败 ❌，不落地")

# ============================================================
# 全部写完后：
#   - 完整跑一遍，确认 GX result.success == True
#   - data/cleaned/tatoeba_clean.parquet 生成
#   - 记得最后 spark.stop()（可选）
#   - 然后叫我批改
# ============================================================
spark.stop()
