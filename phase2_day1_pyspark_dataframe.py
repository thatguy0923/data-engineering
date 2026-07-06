#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2 Day 1-3: PySpark DataFrame API
========================================

学习目标：
  1. 启动 SparkSession
  2. PySpark DataFrame 的创建、查看、筛选、分组
  3. 把 Phase 1 的 ETL 分析用 PySpark 重写

每个关卡给了三道提示：① Pandas 写法（你会） ② PySpark 语法（你要学的） ③ 你自己的代码（空白等你写）
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ================================================================
# 第 1 步：启动 SparkSession
# ================================================================
# ① Pandas:  import pandas as pd
# ② PySpark:  spark = SparkSession.builder.appName("名字").master("local[*]").getOrCreate()
#    - appName: 随便起个名字
#    - master("local[*]"): 用你电脑所有 CPU 核心

print("===== 一、启动 SparkSession =====")

# TODO: 启动 SparkSession
spark = SparkSession.builder.appName("Phase 2 Day 1").master("local[*]").getOrCreate()

# ================================================================
# 第 2 步：创建 DataFrame
# ================================================================
# ① Pandas:  df = pd.DataFrame(data, columns=["name", "dept", "salary"])
# ② PySpark: df = spark.createDataFrame(data, schema=["name", "department", "salary"])
#    - data: Python 列表，每个元素是 tuple
#    - schema: 列名列表
#    - 显示用 df.show()  ← 注意不是 df.head()

print("\n===== 二、创建 DataFrame =====")

data = [
    ("张三", "Engineering", 15000),
    ("李四", "Data", 13000),
    ("王五", "Engineering", 18000),
    ("赵六", "HR", 9000),
    ("钱七", "Engineering", 22000),
    ("孙八", "Data", 16000),
]

# TODO: 创建 DataFrame
df = spark.createDataFrame(data, schema = ["name", "department", "salary"])

# TODO: 用 .show() 显示
df.show()

# ================================================================
# 第 3 步：查看数据
# ================================================================
# ① Pandas:  df.head()       → 看前几行
#            df.info()       → 看类型
#            len(df)         → 行数
# ② PySpark: df.show()        → 看前 20 行
#            df.printSchema() → 看类型结构
#            df.count()       → 行数（⚠️ 不是 len(df)）

print("\n===== 三、查看数据 =====")

# TODO: 三个方法各调用一次
df.show(20)
df.printSchema()
print("行数 =", df.count())

# ================================================================
# 第 4 步：select / filter
# ================================================================
# ① Pandas:  df[['name', 'salary']]                → 选列
#            df[df['salary'] > 12000]               → 筛选
# ② PySpark: df.select("name", "salary")
#            df.select(F.col("name"), (F.col("salary") * 12).alias("annual"))
#              ↑ F.col("列名") 引用列，.alias("别名") 起名字
#            df.filter(F.col("salary") > 12000)
#            df.filter( (条件1) & (条件2) )           ← 注意：用 & 不用 and
#            df.filter(F.col("department") != "HR")

print("\n===== 四、select / filter =====")

# TODO: select "name", "salary"
df.select("name", "salary").show()

# TODO: filter salary > 12000
df.filter(F.col("salary") > 12000).show()

# TODO: filter 多条件：salary >= 10000 且 department 不是 "HR"
df.filter((F.col("salary") >= 10000) & (F.col("department") != "HR")).show()

# ================================================================
# 第 5 步：groupBy / agg
# ================================================================
# ① Pandas:  df.groupby('department').agg(
#                emp_count=('name', 'count'),
#                avg_salary=('salary', 'mean'),
#            )
# ② PySpark: df.groupBy("department").agg(
#                F.count("name").alias("emp_count"),
#                F.avg("salary").alias("avg_salary"),
#                F.max("salary").alias("max_salary"),
#            )
#    - F.count / F.avg / F.sum / F.max / F.min — 聚合函数都在 F 里
#    - .alias("名字") 给结果列起别名

print("\n===== 五、groupBy / agg =====")

# TODO: 按 department 分组，统计人数、平均薪资、最高薪资
df.groupBy("department").agg(
    F.count("name").alias("emp_count"),
    F.avg("salary").alias("avg_salary"),
    F.max("salary").alias("max_salary"), 
).show()

# ================================================================
# 第 6 步：排序 + 限制
# ================================================================
# ① Pandas:  df.sort_values('salary', ascending=False).head(3)
# ② PySpark: df.orderBy(F.col("salary").desc()).limit(3)
#    也可以写成 df.sort(F.col("salary").desc()) — orderBy 和 sort 完全等价
#    .desc() 降序 / .asc() 升序（默认）

print("\n===== 六、排序 + 限制 =====")

# TODO: 按 salary 降序，取前 3 名
df.orderBy(F.col("salary").desc()).limit(3).show()

# ================================================================
# 第 7 步：withColumn + CASE WHEN
# ================================================================
# ① Pandas:  df['annual'] = df['salary'] * 12
#            df['level'] = np.select(conditions, choices, default='基础')
# ② PySpark: df.withColumn("新列名", 计算表达式)
#            df.withColumn("annual_salary", F.col("salary") * 12)
#            df.withColumn("level",
#                F.when(F.col("salary") >= 15000, "高薪")
#                 .when(F.col("salary") >= 10000, "中等")
#                 .otherwise("基础")
#            )
#    - withColumn 返回新 DataFrame（PySpark 的 DataFrame 不可变）
#    - F.when(条件, 值).when(条件, 值).otherwise(默认值) = CASE WHEN

print("\n===== 七、withColumn + CASE WHEN =====")

# TODO: 新增 annual_salary = salary * 12
df.withColumn("annual_salary", F.col("salary") * 12)

# TODO: 新增 salary_level
#       >= 15000 → '高薪'  |  >= 10000 → '中等'  |  否则 → '基础'
df.withColumn("salary_level",
    F.when(F.col("salary") >= 15000, "高薪")
     .when(F.col("salary") >= 10000, "中等")
     .otherwise("基础")
).show()

# ================================================================
# 第 8 步：Spark → Pandas
# ================================================================
# ① Pandas:  （本来就是 Pandas）
# ② PySpark: pandas_df = spark_df.toPandas()
#    ⚠️ toPandas() 把全部数据拉到一台机器内存里，数据大时会炸

print("\n===== 八、转回 Pandas =====")

# TODO: 用 .toPandas() 转成 Pandas DataFrame，然后 print
pandas_df = df.toPandas()
print(pandas_df)

# ================================================================
# 第 9 步：实战 — 窗口函数
# ================================================================
# ① Pandas:  df['rank'] = df.groupby('dept')['salary'].rank(method='first', ascending=False)
#            df['avg'] = df.groupby('dept')['salary'].transform('mean')
# ② PySpark: 需要定义一个窗口（Window），然后在上面挂函数
#
#    # 定义窗口
#    window_spec = Window.partitionBy("department").orderBy(F.col("salary").desc())
#    # partitionBy = Pandas 的 groupby，orderBy = 排序
#
#    # 窗口函数
#    F.row_number().over(window_spec)     → ROW_NUMBER()
#    F.rank().over(window_spec)           → RANK()
#    F.avg("salary").over(Window.partitionBy("department"))  → AVG OVER
#
#    # 用法（和 withColumn 配合）
#    df.withColumn("rank", F.row_number().over(window_spec))
#    df.withColumn("dept_avg", F.avg("salary").over(Window.partitionBy("department")))

print("\n===== 九、窗口函数 =====")

more_data = [
    ("张三", "Engineering", 15000, 8),
    ("李四", "Data", 13000, 5),
    ("王五", "Engineering", 18000, 10),
    ("赵六", "HR", 9000, 3),
    ("钱七", "Engineering", 22000, 12),
    ("孙八", "Data", 16000, 7),
    ("周九", "Product", 12000, 4),
    ("吴十", "Marketing", 11000, 6),
    ("郑十一", "Product", 14000, 8),
    ("陈十二", "Engineering", 5000, 1),
]
more_df = spark.createDataFrame(more_data, schema=["name", "department", "salary", "years"])

# TODO: 1. 定义窗口：按 department 分区，按 salary 降序
window_spec = Window.partitionBy("department").orderBy(F.col("salary").desc())

# TODO: 2. 部门内排名（用 row_number）
rank_df = more_df.withColumn("emp_rank", F.row_number().over(window_spec))

# TODO: 3. 部门均值（用 F.avg + Window）
avg_df = rank_df.withColumn("salary_avg", F.avg("salary").over(window_spec))

# TODO: 4. 人才标签：salary >= 15000 且 years >= 8 → "核心人才"，否则 "普通员工"
#        提示：用 F.when((条件1) & (条件2), "核心人才").otherwise("普通员工")
final_df = avg_df.withColumn("salary_rank",
    F.when((F.col("salary") >= 15000) & (F.col("years") >= 8), "核心人才")
     .otherwise("普通员工")
)

# TODO: 5. orderBy("department", F.col("salary").desc()) 排序后 show()
final_df.orderBy("department", F.col("salary").desc()).show()

# ================================================================
# 完成
# ================================================================

print("\n===== 完成 =====")
