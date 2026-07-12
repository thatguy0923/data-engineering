#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2 Day 12: Great Expectations 1.x —— 数据质量自动化
=========================================================

⚠️ 版本说明（重要）：
  你装的是 Great Expectations 1.18（也叫 GX 1.x）。
  网上很多老教程用的是 0.18 版，命令是 `great_expectations init`、
  `great_expectations suite new` 这种 CLI —— 1.x 把 CLI 全删了，
  只剩 Python API。所以这份文件全部用 Python 代码，不用命令行。

  记住这个版本差异，面试/查文档时不要被老教程带偏。

学习目标：
  1. 理解数据质量框架是什么，为什么不能只靠手工检查
  2. 用 GX 1.x 的 Python API 给管道加自动化质量断言
  3. 产出自动化的数据质量 HTML 报告

场景：退款 ETL 管道跑完之后，自动验证数据质量
  之前 validate_results() 是手写的几个 if 检查
  现在用 GX 框架来做 —— 验证规则可复用、可扩展、自动生成报告

核心理念：
  写的不是"检查代码"，而是"数据应该长什么样的声明"

  你写: "refund_rate 应该在 0 到 1 之间"
  GX 做: 自动跑这条规则 + 记录哪行没过 + 生成 HTML 报告 + 记入历史
"""

# ================================================================
# 第 1 关：理解"声明式验证"和"命令式验证"的区别
# ================================================================
print("===== 第 1 关：两种验证方式 =====")

print("""
命令式验证（你之前做的）：
  if df.filter(F.col("refund_amount") < 0).count() > 0:
      raise PipelineError("存在负数")
  → 你自己写"怎么检查"，每加一条规则就多加一个 if

声明式验证（GX 的方式）：
  ExpectColumnMinToBeBetween(column="refund_amount", min_value=0)
  → 你声明"这列的最小值应该在 0 以上"，框架自己跑检查

为什么重要？
  - 20 个 if 的 validate 函数 → 维护成本越来越高
  - 每次改规则要改代码，容易出错
  - 面试官问"你的数据质量怎么保证" → 你说"我用 Great Expectations"
    比说"我写了几个 if"强一个级别

  注意：GX 不是为了"更快"（它比裸 if 慢，有框架开销），
  而是为了可读、可复用、有报告、有历史、规则和代码分离。
""")

# TODO 1：用你自己的话写一下 —— 声明式验证比命令式好在哪？
# （至少两个理由）首先是可读性强，并且维护成本低


# ================================================================
# 第 2 关：GX 1.x 的四层核心概念
# ================================================================
print("\n===== 第 2 关：GX 1.x 的骨架 =====")

print("""
GX 1.x 不用初始化命令了，直接在 Python 里搭。核心是四层：

  ┌──────────────────────────────────────────────────────┐
  │ 1. Context（上下文）                                  │
  │    整个 GX 项目的入口，管理所有配置                    │
  │    context = gx.get_context(mode="file")             │
  │    → mode="file" 会在 ./gx/ 目录持久化配置            │
  │    → mode="ephemeral" 只存内存，退出就没（测试用）    │
  ├──────────────────────────────────────────────────────┤
  │ 2. Data Source → Data Asset → Batch Definition       │
  │    告诉 GX "数据从哪来"                                │
  │    data_source: 数据源类型（pandas / spark / sql）    │
  │    data_asset:  具体一份数据                           │
  │    batch_definition: 怎么切批次（整份 or 按日期分）    │
  ├──────────────────────────────────────────────────────┤
  │ 3. Expectation Suite（期望套件）                      │
  │    一组验证规则的集合                                  │
  │    suite = ExpectationSuite(name="refund_suite")     │
  │    suite.add_expectation(ExpectColumn...())          │
  ├──────────────────────────────────────────────────────┤
  │ 4. Validation Definition + Checkpoint                │
  │    把"数据(batch)" + "规则(suite)"绑在一起去跑        │
  │    ValidationDefinition(data=batch_def, suite=suite) │
  │    Checkpoint = 一次或多次验证 + 触发动作(出报告)      │
  └──────────────────────────────────────────────────────┘

一句话串起来：
  Context 里，把 [某份数据的批次] 和 [一套期望规则] 组成一个
  ValidationDefinition，再用 Checkpoint 跑它并生成报告。
""")

# TODO 2：不看上面，用自己的话默写这四层分别是干什么的
# 特别想清楚：Data Source / Suite / ValidationDefinition / Checkpoint
# 这四个概念面试常问，要能一句话说清各自的职责
#第一层就是管理所有gx的配置的文件，第二层就是写着数据的类型以及是那一份数据，以及按什么划分批次，第三层就是检验规则，第四层就是把数据和检验规则一起跑，验证加报告告警


# ================================================================
# 第 3 关：连接数据源（先用 pandas 跑通，再换 Spark）
# ================================================================
print("\n===== 第 3 关：连接数据源 =====")

print("""
GX 1.x 有三种引擎，你的项目相关的：

  add_pandas(name)   → 验证 pandas DataFrame（最简单，先用这个跑通）
  add_spark(name)    → 验证 PySpark DataFrame（你退款项目用的）
  add_sql(name, connection_string) → 直接连 MySQL 验证表

推荐路径：先用 pandas 把整个流程跑通，理解概念，
再换成 Spark（API 几乎一样，只是 add_pandas → add_spark）。

pandas 数据源的三步（固定套路）：
  ds    = context.data_sources.add_pandas(name="refund_ds")
  asset = ds.add_dataframe_asset(name="refund_asset")
  bd    = asset.add_batch_definition_whole_dataframe("refund_bd")

关键点：DataFrame 本身不在这里传！
  这里只是"定义结构"，真正的 df 是在最后 run() 时传进去：
  vd.run(batch_parameters={"dataframe": df})
  → 这样同一套配置能反复用在不同日期的数据上
""")

# TODO 3-1：写代码创建 context（mode="file"）
# 然后用 add_pandas 三步套路，定义一个数据源
# 提示：import great_expectations as gx
import pandas as pd
import great_expectations as gx
context = gx.get_context(mode = "file")
ds = context.data_sources.add_pandas(name = "refund_ds")
asset = ds.add_dataframe_asset(name = "refund_asset")
bd = asset.add_batch_definition_whole_dataframe("refund_bd")

# TODO 3-2：造一个测试用的 pandas DataFrame，模拟 daily_refund_report
# 至少包含这几列：product_category, total_orders, refund_rate, total_refund
# （后面第 5 关会故意塞脏数据进去测试）

df = pd.DataFrame({
    "product_category": ["Electronics", "Clothing", "Books"],
    "total_orders": [100, 200, 150],
    "refund_rate": [0.1, 0.2, 0.15],
    "total_refund": [1000, 2000, 1500],
    "report_date": ["2026-07-07", "2026-07-07", "2026-07-07"],
})

# ================================================================
# 第 4 关：写第一组验证规则（Expectation Suite）
# ================================================================
print("\n===== 第 4 关：日常数据质量检查 =====")

print("""
对 daily_refund_report 这张表，每天跑完管道后至少要验证：

  常规检查（对应的 Expectation 类名）：
    1. total_orders 最小值 > 0
       → ExpectColumnMinToBeBetween(column="total_orders", min_value=1)
    2. total_refund 非负
       → ExpectColumnValuesToBeBetween(column="total_refund", min_value=0)
    3. refund_rate 在 0~1 之间
       → ExpectColumnValuesToBeBetween(column="refund_rate", min_value=0, max_value=1)
    4. product_category 不能为 NULL
       → ExpectColumnValuesToNotBeNull(column="product_category")
    5. 某列必须存在
       → ExpectColumnToExist(column="report_date")

  怎么组装：
    from great_expectations.expectations import (
        ExpectColumnMinToBeBetween,
        ExpectColumnValuesToBeBetween,
        ExpectColumnValuesToNotBeNull,
    )
    suite = context.suites.add(gx.ExpectationSuite(name="refund_quality_suite"))
    suite.add_expectation(ExpectColumnValuesToBeBetween(
        column="refund_rate", min_value=0, max_value=1))
    # ... 继续 add 其他规则

  常用 Expectation 速查（GX 1.x 都是驼峰类名）：
    ExpectColumnToExist                     列存在
    ExpectColumnValuesToNotBeNull           非空
    ExpectColumnValuesToBeBetween           逐行值在范围内
    ExpectColumnMinToBeBetween              最小值在范围内
    ExpectColumnMaxToBeBetween              最大值在范围内
    ExpectColumnMeanToBeBetween             均值在范围内（异常检测）
    ExpectColumnValuesToBeInSet             值在给定集合内（枚举校验）
    ExpectColumnValuesToBeUnique            唯一（主键校验）
""")

# TODO 4-1：创建一个 ExpectationSuite，至少加 5 条规则
# 用上面列的常规检查 1~5，对应你第 3-2 造的 DataFrame 的列
from great_expectations.expectations import (
    ExpectColumnMinToBeBetween,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToNotBeNull,
    ExpectColumnToExist,
    ExpectColumnMaxToBeBetween,
    ExpectColumnValuesToBeInSet,
)
suite = context.suites.add(gx.ExpectationSuite(name = "refund_quality_suite"))
suite.add_expectation(ExpectColumnMinToBeBetween(column = "total_orders", min_value = 1))
suite.add_expectation(ExpectColumnValuesToBeBetween(column = "total_refund", min_value = 0))
suite.add_expectation(ExpectColumnValuesToBeBetween(column = "refund_rate", min_value = 0, max_value = 1))
suite.add_expectation(ExpectColumnValuesToNotBeNull(column = "product_category"))
suite.add_expectation(ExpectColumnToExist(column = "report_date"))

# TODO 4-2：再加一条业务规则 —— 验证 total_refund 最大值不超过 50000
# 提示：ExpectColumnMaxToBeBetween(column=..., max_value=50000)
suite.add_expectation(ExpectColumnMaxToBeBetween(column = "total_refund", max_value = 50000))
# TODO 4-3：想一条你自己觉得该加的规则（比如品类只能是某几个值）
# 用 ExpectColumnValuesToBeInSet 实现
suite.add_expectation(ExpectColumnValuesToBeInSet(column = "product_category", value_set = ["Electronics", "Clothing", "Books"]))

# ================================================================
# 第 5 关：跑验证 + 看报告
# ================================================================
print("\n===== 第 5 关：执行验证 =====")

print("""
GX 1.x 执行验证有两种粒度：

  A. ValidationDefinition.run() —— 跑一次，拿结果对象
     vd = context.validation_definitions.add(
         gx.ValidationDefinition(name="refund_vd", data=bd, suite=suite))
     result = vd.run(batch_parameters={"dataframe": df})
     print(result.success)          # True / False
     print(result.statistics)       # 通过几条、失败几条

  B. Checkpoint —— 生产用的方式，验证 + 自动触发动作（出 HTML 报告）
     from great_expectations.checkpoint import UpdateDataDocsAction
     cp = context.checkpoints.add(gx.Checkpoint(
         name="refund_checkpoint",
         validation_definitions=[vd],
         actions=[UpdateDataDocsAction(name="update_docs")],
     ))
     res = cp.run(batch_parameters={"dataframe": df})

跑完之后：
  - HTML 报告自动生成在 gx/uncommitted/data_docs/local_site/
  - 用 context.open_data_docs() 可以直接在浏览器打开
  - 失败的规则有详细的"期望值 vs 实际值"对比
  - 每次结果记入历史，能看趋势
""")

# TODO 5-1：用 ValidationDefinition.run() 跑一次验证
# 打印 result.success 和 result.statistics，确认全过
vd = context.validation_definitions.add(
    gx.ValidationDefinition(name = "refund_vd", data = bd, suite = suite)
)
result = vd.run(batch_parameters = {"dataframe": df})
print(result.success)
print(result.statistics)

# TODO 5-2：建一个 Checkpoint，带 UpdateDataDocsAction，跑一次
# 然后 context.open_data_docs() 打开 HTML 报告，看看长什么样
from great_expectations.checkpoint import UpdateDataDocsAction
cp = context.checkpoints.add(gx.Checkpoint(
    name = "refund_checkpoint",
    validation_definitions = [vd],
    actions = [UpdateDataDocsAction(name = "update_docs")],
))
res = cp.run(batch_parameters = {"dataframe": df})
context.open_data_docs()
# TODO 5-3：故意把 DataFrame 里塞一条脏数据（比如 refund_rate = 1.5）
# 再跑一次 checkpoint，看报告会不会标红失败
# —— 这一步是验证"你的验证"本身是有效的，很重要
df_e = pd.DataFrame({
    "product_category": ["Electronics", "Clothing", "Books"],
    "total_orders": [100, 200, 150],
    "refund_rate": [0.1, 1.5, 0.15],  # 故意塞脏数据
    "total_refund": [1000, 2000, 1500],
    "report_date": ["2026-07-07", "2026-07-07", "2026-07-07"],
})
res = cp.run(batch_parameters = {"dataframe": df_e})
context.open_data_docs()
# ================================================================
# 第 6 关：从 pandas 换到 Spark（对接你的退款管道）
# ================================================================
print("\n===== 第 6 关：换成 Spark 引擎 =====")

print("""
把上面的 pandas 版改成 Spark，几乎只改一个词：

  # pandas 版
  ds = context.data_sources.add_pandas(name="refund_ds")

  # Spark 版
  ds = context.data_sources.add_spark(name="refund_ds")

后面 add_dataframe_asset / add_batch_definition_whole_dataframe
完全一样。run() 时传的也是同一个 key：
  vd.run(batch_parameters={"dataframe": spark_df})
  ← 这里的 spark_df 是你 refund_etl.py 里的 PySpark DataFrame

对接思路：
  在 refund_etl.py 的 validate 步骤里，
  聚合完拿到最终的 spark_df 之后，不再手写 if，
  改成走一遍 GX checkpoint。
""")

# TODO 6-1：把第 3~5 关的代码复制一份，把 add_pandas 改成 add_spark
# 用一个小的 spark DataFrame 测试跑通（可以从 refund_etl 里借一段数据）
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("GX Test").getOrCreate()
df_spark = spark.createDataFrame(df)
ds_spark = context.data_sources.add_spark(name = "refund_spark_ds")
asset_spark = ds_spark.add_dataframe_asset(name = "refund_spark_asset")
bd_spark = asset_spark.add_batch_definition_whole_dataframe("refund_spark_bd")
vd_spark = context.validation_definitions.add(
    gx.ValidationDefinition(name = "refund_spark_vd", data = bd_spark, suite = suite)
)
result_spark = vd_spark.run(batch_parameters = {"dataframe": df_spark})
print(result_spark.success)
print(result_spark.statistics)
cp_spark = context.checkpoints.add(gx.Checkpoint(
    name = "refund_spark_checkpoint",
    validation_definitions = [vd_spark],
    actions = [UpdateDataDocsAction(name = "update_docs")],
))
res = cp_spark.run(batch_parameters = {"dataframe": df_spark})
context.open_data_docs()

# TODO 6-2（选做）：把 GX 验证封装成一个函数 run_ge_check(spark_df)
# 返回 bool（是否全过），失败时抛 PipelineError
# 这样能塞进你现有的 refund_etl.py validate 步骤

from refund_config import PipelineError

def run_ge_check(spark_df):
    ctx = gx.get_context(mode="ephemeral")

    # Spark 数据源三步
    ds = ctx.data_sources.add_spark(name="ge_ds")
    asset = ds.add_dataframe_asset(name="ge_asset")
    bd = asset.add_batch_definition_whole_dataframe("ge_bd")

    # 验证规则（跟你第 4 关写的 5 条核心规则一致）
    suite = ctx.suites.add(gx.ExpectationSuite(name="ge_suite"))
    suite.add_expectation(ExpectColumnMinToBeBetween(column="total_orders", min_value=1))
    suite.add_expectation(ExpectColumnValuesToBeBetween(column="total_refund", min_value=0))
    suite.add_expectation(ExpectColumnValuesToBeBetween(column="refund_rate", min_value=0, max_value=1))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="product_category"))
    suite.add_expectation(ExpectColumnToExist(column="report_date"))

    # 绑定数据 + 规则，跑验证
    vd = ctx.validation_definitions.add(
        gx.ValidationDefinition(name="ge_vd", data=bd, suite=suite))
    result = vd.run(batch_parameters={"dataframe": spark_df})

    if not result.success:
        stats = result.statistics
        raise PipelineError(
            f"数据质量验证失败: {stats['unsuccessful_expectations']}/{stats['evaluated_expectations']} 条未通过"
        )

    return True


# ── 测试 run_ge_check ──
print("\n===== 测试 run_ge_check =====")

# 测试 1：干净数据 → 应该返回 True
print("测试1（干净数据）:", run_ge_check(df_spark))

# 测试 2：脏数据 → 应该抛 PipelineError
df_dirty = spark.createDataFrame(
    pd.DataFrame({
        "product_category": ["Electronics"],
        "total_orders": [0],          # ← 故意违规：最小值小于 1
        "refund_rate": [0.1],
        "total_refund": [1000],
        "report_date": ["2026-07-07"],
    })
)
try:
    run_ge_check(df_dirty)
    print("测试2（脏数据）: ❌ 没抛异常，有问题！")
except PipelineError as e:
    print(f"测试2（脏数据）: ✅ 正确抛出 PipelineError → {e}")


# ================================================================
# 第 7 关：对接 Airflow
# ================================================================
print("\n===== 第 7 关：GX 集成到 Airflow DAG =====")

print("""
管道流程变成：

  新流程：
  ① PySpark ETL 完成
  ② GX Checkpoint 自动跑
  ③ 如果验证全过 → DAG 标记为 success
  ④ 如果验证不过 → raise 异常 → DAG 失败 → 触发告警
  ⑤ HTML 报告自动存档

  旧流程：
  ① PySpark ETL
  ② validate_results() ← 只有 4 个 if
  ③ 没了

用 PythonOperator 在 DAG 里加一个任务，直接调 GX 的 Python API：

  def run_quality_check(**context):
      import great_expectations as gx
      ctx = gx.get_context(mode="file")
      cp = ctx.checkpoints.get("refund_checkpoint")
      # 注意：Airflow 里 Spark df 不好跨任务传，
      # 常见做法是这一步重新读 MySQL 结果表成 df 再验证，
      # 或者用 add_sql 直接连 MySQL 表验证（不用传 df）
      result = cp.run(...)
      if not result.success:
          raise ValueError("数据质量验证未通过")

  quality_check = PythonOperator(
      task_id="quality_check",
      python_callable=run_quality_check,
  )
  run_etl >> quality_check

  💡 Airflow 场景下，用 add_sql 直连 MySQL 表验证往往比传 df 更省事，
     因为不用在任务之间传递 Spark DataFrame。
""")

# TODO 7-1：想清楚一个问题 —— 为什么 Airflow 里用 add_sql 连 MySQL
# 比传 Spark DataFrame 更方便？（提示：任务之间数据怎么传递）
# Airflow 每个 task 是独立进程,内存不共享。Spark DataFrame 活在
# run_etl 任务的内存里,任务一结束就没了,没法通过 XCom 传给下游
# (XCom 只能传小数据)。而 run_etl 最后已经把结果写进 MySQL 表,
# quality_check 用 add_sql 直接连表读就行,数据现成的,不用跨任务传 df。

# TODO 7-2（选做）：在你的退款 DAG 里加一个 quality_check 任务
# 用 add_sql 连 MySQL 的 daily_refund_report 表来验证
# 设 run_etl >> quality_check，手动触发确认跑通


# ================================================================
# 学完自检
# ================================================================
print("\n===== 学完自检 =====")
questions = [
    "GX 1.x 和 0.18 老版最大的区别是什么？（提示：CLI）",
    "GX 的四层：Context / DataSource / Suite / Checkpoint 分别是什么？",
    "声明式验证和命令式验证的区别？GX 慢一点为什么还值得用？",
    "batch_parameters={'dataframe': df} 里的 df 为什么在 run 时才传，不在配置里写死？",
    "从 pandas 换到 Spark 引擎，代码要改哪里？",
    "Airflow 里为什么 add_sql 连表比传 Spark df 更省事？",
    "面试官问'你怎么保证数据质量'，用 3 句话回答",
]
for i, q in enumerate(questions, 1):
    print(f"  {i}. {q}")

print("\n🚀 下一步 → Day 13：DVC 数据版本控制")
