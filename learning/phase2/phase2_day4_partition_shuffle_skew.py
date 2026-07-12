#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2 Day 4-5: PySpark 分区 / Shuffle / 数据倾斜
====================================================

学习目标：
  1. 理解分区是什么，怎么控制
  2. 区分窄依赖和宽依赖（Shuffle 边界）
  3. 发现和修复数据倾斜

关键心态：这三个概念是绑在一起的——
  分区是"数据分成几块"
  Shuffle 是"数据在块之间搬家的代价"
  数据倾斜是"某一块特别大 → Shuffle 特别慢"

每个关卡给了：① 概念 ② PySpark 语法 ③ 你要写的代码
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

spark = SparkSession.builder \
    .appName("Phase 2 Day 4-5") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ================================================================
# 第 1 关：认识分区
# ================================================================
# 概念：
#   分区（Partition）= 数据的最小并行单元
#   一个分区 = 一个 CPU 核心同一时刻处理的那块数据
#   分区越多 → 并行度越高，但每个分区太小 → 调度开销 > 计算收益
#
#   类比：100 页书
#   - 1 个分区 = 1 个人读完 100 页
#   - 4 个分区 = 4 个人各读 25 页 → 并行，快 4 倍
#   - 100 个分区 = 100 个人各读 1 页 → 分书的时间比读书长，反而慢
#
# 关键 API：
#   df.rdd.getNumPartitions()     → 看当前有几个分区
#   df.repartition(n)             → 重分区（= 全量 Shuffle，贵）
#   df.coalesce(n)                → 减少分区（不 Shuffle，便宜）
#   spark.sparkContext.defaultParallelism  → 默认并行度

print("===== 一、认识分区 =====")

# 造一批数据：1000 万行，只有一列数字
# 提示：用 spark.range(10000000)
df_big = spark.range(10_000_000)

# TODO 1: 看初始分区数
# 提示: df_big.rdd.getNumPartitions()
print("初始分区数:", df_big.rdd.getNumPartitions())  # 你的代码


# TODO 2: 把 df_big 重分区到 4 个分区，然后 count()
# 提示: df_big.repartition(4)
# 观察：repartition 后 count 有没有变慢？为什么？
df_4 = df_big.repartition(4)
print("repartition(4) 分区数:", df_4.rdd.getNumPartitions())
print("行数:", df_4.count())


# TODO 3: 用 coalesce(1) 把 4 个分区合并成 1 个
# 提示: df_4.coalesce(1)
# coalesce 和 repartition(1) 的区别是什么？（思考题，写在注释里） 一个是拆分，一个是合并
df_1 = df_4.coalesce(1)
print("coalesce(1) 分区数:", df_1.rdd.getNumPartitions())


# TODO 4: 分别对 df_big（多分区）和 df_1（1 分区）做 filter + count，对比耗时
# 提示: 用 time.time() 记录前后时间
# 写你的观察：哪个更快？为什么？

# 你的代码
start_1 = time.time()
df_big.filter(F.col("id") >= 1000).count() #id这个名字是生成的时候自带的
end_1 = time.time()
print(f"{end_1 - start_1 : .2f}")

start_2 = time.time()
df_1.filter(F.col("id") >= 1000).count()
end_2 = time.time()
print(f"{end_2 - start_2 : .2f}")

# ================================================================
# 第 2 关：窄依赖 vs 宽依赖（Shuffle 边界）
# ================================================================
# 概念：
#   窄依赖（Narrow）：每个子分区只依赖 1 个父分区
#     → 不需要跨分区搬数据 → 快
#     map / filter / select / withColumn 都是窄依赖
#
#   宽依赖（Wide）：每个子分区依赖多个父分区
#     → 需要 Shuffle（数据跨网络/磁盘重分布） → 慢
#     groupBy / join / distinct / orderBy 都是宽依赖
#
#   类比：教室里的学生（每排 = 一个分区）
#   - 窄依赖 = 每排自己传卷子（filter）→ 不用起身
#   - 宽依赖 = 按身高重新排队（groupBy）→ 所有人起身，重新站
#
# Spark UI 怎么看 Shuffle：
#   程序运行中打开 http://localhost:4040
#   → Stages 标签 → 看到 Shuffle Write / Shuffle Read 的字节数
#   → Shuffle Read 越大 = 搬的数据越多 = 越慢

print("\n===== 二、窄依赖 vs 宽依赖 =====")

# 造一份员工数据
emp_data = [
    ("张三", "Engineering", 15000),
    ("李四", "Data", 13000),
    ("王五", "Engineering", 18000),
    ("赵六", "HR", 9000),
    ("钱七", "Engineering", 22000),
    ("孙八", "Data", 16000),
    ("周九", "Product", 12000),
    ("吴十", "Marketing", 11000),
    ("郑十一", "Product", 14000),
    ("陈十二", "Engineering", 5000),
]
emp_df = spark.createDataFrame(emp_data, schema=["name", "department", "salary"])

# TODO 5: 标出以下操作是窄依赖还是宽依赖（写在注释里就行，不用运行）
# 做完后再运行验证你的判断

# a) emp_df.select("name", "salary")                        → 窄/宽？窄
# b) emp_df.filter(F.col("salary") > 10000)                 → 窄/宽？窄
# c) emp_df.withColumn("annual", F.col("salary") * 12)      → 窄/宽？窄
# d) emp_df.groupBy("department").agg(F.avg("salary"))      → 窄/宽？宽
# e) emp_df.orderBy(F.col("salary").desc())                 → 窄/宽？宽
# f) emp_df.select("department").distinct()                 → 窄/宽？宽

# TODO 6: 分别运行一个窄依赖和一个宽依赖操作，感受速度差异
# 窄依赖：filter + select + withColumn 链式调用
# 宽依赖：groupBy + agg

# 窄依赖链（你的代码）
# 提示：emp_df.filter(...).select(...).withColumn(...)
start = time.time()
emp_df.select("salary").filter(F.col("salary") >= 15000).withColumn("plus", F.col("salary") * 2).show
end = time.time()
print(f"{end - start : .2f}")
# 宽依赖（你的代码）
# 提示：emp_df.groupBy("department").agg(...)
start = time.time()
emp_df.groupBy("department").agg(
    F.max("salary"),
    F.count("salary")
).show
end = time.time()
print(f"{end - start : .2f}")
# 观察：哪个更快？在你的数据量下差距大吗？ 现在时间是差不多快，在现在这个数据量下没有什么差别
# 思考：如果数据量是 100 亿行，差距会怎么变？ 数据量大的时候，窄依赖会更快


# ================================================================
# 第 3 关：Shuffle 的三个真正常见场景
# ================================================================
# 场景 1: groupBy → 相同 key 的数据必须搬到同一分区
# 场景 2: join    → 两个表的相同 key 必须搬到同一分区
# 场景 3: distinct / orderBy → 需要全局排序或去重
#
# 优化原则：Shuffle 不能完全避免，但可以——
#   1. 减少 Shuffle 次数（把 filter 放在 groupBy 前面）
#   2. 减少 Shuffle 数据量（只 select 需要的列再 groupBy）
#   3. 用 broadcast join 避免大表 Shuffle（后面会讲）

print("\n===== 三、Shuffle 优化 =====")

# TODO 7: 对比"先 groupBy 再 filter"vs"先 filter 再 groupBy"
# 用 emp_df：
#   A 方案：先 groupBy("department").agg(F.avg("salary"))，再 filter avg_salary > 12000
#   B 方案：先 filter salary > 0（模拟真实业务筛选），再 groupBy department
#
# 思考：如果原始数据 10 亿行，filter 后剩 100 万行，两种方案 Shuffle 的数据量差多少？方案a是shuffle10亿行，方案bshuffle100万行

# 你的代码
emp_df.groupBy("department").agg(
    F.avg("salary").alias("avg_salary")
    ).filter(F.col("avg_salary") > 12000).show()

emp_df.filter(F.col("salary") > 0).groupBy("department").agg(
    F.avg("salary").alias("avg_salary")
).show()


# TODO 8（思考题，写注释）：
# 以下两个操作，哪个 Shuffle 的数据更少？
# a) df.select("department", "salary").groupBy("department").agg(F.avg("salary"))
# b) df.groupBy("department").agg(F.avg("salary")).select("department", "avg(salary)")
#
# 答案写在这里：________a
# 为什么：________先用窄依赖select筛选后用宽依赖groupby能减少shuffle的工作量


# ================================================================
# 第 4 关：数据倾斜（核心难点）
# ================================================================
# 概念：
#   数据倾斜 = 某个 key 的数据量远大于其他 key
#   后果：那个分区跑特别久，其他分区早就空了在等 → 整体时间被拖死
#
#   类比：10 个收银台，9 个没人，1 个排了 1000 人
#   你老板问"为什么总吞吐这么慢"——不是人不够，是分布不均
#
# 倾斜是怎么来的：
#   - 电商：少量热门商品占了 90% 的订单（长尾分布）
#   - 社交：少量大 V 占了 90% 的粉丝
#   - 电商 GMV：NULL 值全聚到一个分区

print("\n===== 四、数据倾斜 =====")

# 造一份故意倾斜的数据：一个热门部门 + 多个冷门部门
import random
random.seed(42)

skewed_data = []
# 热门 key：Engineering 占 60%
for i in range(6000):
    skewed_data.append((f"员工{i}", "Engineering", random.randint(5000, 30000)))
# 20 个冷门 key：各占 2%
for dept_id in range(20):
    for i in range(100):
        skewed_data.append((f"员工{dept_id}_{i}", f"Dept_{dept_id}", random.randint(5000, 30000)))

skewed_df = spark.createDataFrame(skewed_data, schema=["name", "department", "salary"])
skewed_df = skewed_df.repartition(4)  # 先分 4 个区

print(f"总行数: {skewed_df.count()}")
print(f"分区数: {skewed_df.rdd.getNumPartitions()}")

# TODO 9: 诊断倾斜 —— 看每个 department 有多少行
# 提示：groupBy("department").count().orderBy(F.col("count").desc()).show(25)
# 观察：Engineering 比其他 dept 多多少倍？
# 你的代码
skewed_df.groupBy("department").count().orderBy(F.col("count").desc()).show(25)

# TODO 10: 感受倾斜的影响 —— 对比两个 groupBy 聚合的速度
# A: 对 department 做 groupBy + 聚合（有倾斜，因为 Engineering 一个 key 占了 60%）
# B: 对 name 做 groupBy + 聚合（无倾斜，name 基本均匀分布）
#
# 提示：都做 count 即可，用 time.time() 计时
# 观察：B 比 A 快多少？

# 你的代码
start = time.time()
skewed_df.groupBy("department").count().show()
end = time.time()
print(f"{end - start : .2f}")

start = time.time()
skewed_df.groupBy("name").count().show()
end = time.time()
print(f"{end - start : .2f}")

# ================================================================
# 第 5 关：修复数据倾斜 —— 加盐法（Salting）
# ================================================================
# 概念：
#   加盐 = 把一个大 key 人工拆成多个小 key
#
#   原始：Engineering → 全部数据进 1 个分区
#   加盐：Engineering_0, Engineering_1, Engineering_2 → 分散到 3 个分区
#
#   类比：排队太长 → 把一条队拆成三条，分别处理
#
# 步骤：
#   1. 给每行加一个随机后缀（0~N-1），让大 key 散开
#   2. 做 groupBy（现在分散了，不倾斜了）
#   3. 把后缀去掉，再聚合一次（第二次数据量已经小了）

print("\n===== 五、修复倾斜：加盐法 =====")
from pyspark.sql.functions import rand
SALT_COUNT = 5  # 拆成 5 份

# TODO 11: 实现加盐法
# 步骤 1: 造一个 salted_key 列
#   对热门 key 加随机后缀（0~SALT_COUNT-1），冷门 key 保持不变
#   怎么判断"热门"？可以先算每个 department 的 count，大于阈值的就是热门
#
# 提示：
#   from pyspark.sql.functions import rand
#   阈值取 500（先用 500 试试，实际生产按数据量调）
#   热门 key → concat(department, lit("_"), (rand() * SALT_COUNT).cast("int"))
#   冷门 key → 直接用 department

# 步骤 1: 计算每个 department 的行数，判断哪些是热门
# 你的代码（算 dept 行数，标记热门，加盐）
start = time.time()
dept_count = skewed_df.groupBy("department").count()
salted_df = skewed_df.join(dept_count, on="department")
salted_df = salted_df.withColumn("salted_key", 
    F.when(F.col("count") > 500, F.concat(F.col("department"), F.lit("_"), (F.rand() * SALT_COUNT).cast("int")))
     .otherwise(F.col("department"))
)
# 步骤 2: 用 salted_key 做 groupBy 聚合
# 你的代码（按 salted_key 分组聚合）

step2 = salted_df.groupBy("salted_key").agg(
    F.avg("salary").alias("avg_salary"),
    F.count("name").alias("emp_count")
)

# 步骤 3: 去掉后缀，再聚合一次（按原始 department 合并结果）
# 你的代码（第二次聚合）

step3 = step2.withColumn("dept",
    F.when(F.col("salted_key").contains("_"), F.split(F.col("salted_key"), "_")[0])
     .otherwise(F.col("salted_key"))
).groupBy("dept").agg(
    F.sum("avg_salary"),
    F.sum("emp_count") #这里只是做个示范，没有严格的数学逻辑
).show()
end = time.time()
print(f"{end - start :.2f}")
# 步骤 4: 对比加盐前和加盐后的 groupBy 耗时
# 你的代码（用 time.time() 计时对比）
start = time.time()
skewed_df.groupBy("department").agg(
    F.avg("salary").alias("avg_salary"),
    F.count("name").alias("emp_count")
).show()
end = time.time()
print(f"{end - start : .2f}")
# 思考题（写注释）：
# 1. SALT_COUNT 设太大（比如 1000）会有什么问题？分区太多，就像前面提到的分区，分太多会影响效率
# 2. 加盐法适合什么场景，不适合什么场景？ 数据本来就均匀以及不想改变key的数据，加盐后key会变化（key就是你groupby的那一列）
# 3. 如果倾斜的原因是 NULL 值太多怎么办？（提示：NULL 不需要聚合结果） 用isnull或isnotnull


# ================================================================
# 第 6 关：Day 4-5 综合挑战
# ================================================================
# 场景：电商订单数据，order_id 是均匀的，但 user_id 有少数"鲸鱼用户"
# 下了大量订单。你要统计每个用户的订单数和总金额。

print("\n===== 六、综合挑战 =====")

# 造订单数据：10001 个用户，50000 笔订单
# 其中 user_id=0 的"鲸鱼用户"占了 20% 的订单（10000 笔）
# 其余 10000 个用户瓜分剩余 80%（40000 笔，每人平均 4 笔）

random.seed(123)
orders = []
# 鲸鱼用户 0：10000 单
for i in range(10000):
    orders.append((i, 0, round(random.uniform(10, 500), 2)))
# 其余 10000 个用户：共 40000 单
for i in range(10000, 50000):
    uid = random.randint(1, 10000)
    orders.append((i, uid, round(random.uniform(10, 500), 2)))

orders_df = spark.createDataFrame(orders, schema=["order_id", "user_id", "amount"])
orders_df = orders_df.repartition(8)

# TODO 12（综合题）：
# a) 诊断倾斜：看每个 user_id 的订单数分布
# b) 直接 groupBy user_id 聚合（sum amount, count order_id），记录耗时
# c) 用加盐法优化（自己判断 SALT_COUNT 该设多少，写清楚理由）
# d) 对比优化前后的耗时
# e) 挑战：不加盐，用 reduceByKey 的思维手动实现一个两阶段聚合
#    （提示：第一阶段按 (user_id + 随机后缀) 聚合 → 第二阶段按 user_id 聚合）
#
# 要求：你自己设计实验，输出对比结果

# 你的代码在这里
user_count = orders_df.groupBy("user_id").count().orderBy(F.col("count").desc())
user_count.show()
start = time.time()
origin = orders_df.groupBy("user_id").agg(
    F.sum("amount").alias("total_amount"),
    F.count("order_id").alias("total_order")
).show()
end = time.time()
print(f"{end - start :.2f}")
start = time.time()
SALT_COUNT = 4
reducebykey = orders_df.join(user_count, on="user_id")
reducebykey = reducebykey.withColumn("salted_key",
    F.when(F.col("count") > 250, F.concat(F.col("user_id").cast("string"), F.lit("_"), (F.rand() * SALT_COUNT).cast("int").cast("string")))
     .otherwise(F.col("user_id").cast("string"))
)
step2 = reducebykey.groupBy("salted_key").agg(
    F.sum("amount").alias("total_amount"),
    F.count("order_id").alias("total_order")
)
step3 = step2.withColumn("id",
    F.when(F.col("salted_key").contains("_"), F.split(F.col("salted_key"), "_")[0])
     .otherwise(F.col("salted_key"))
).groupBy("id").agg(
    F.sum("total_amount"),
    F.sum("total_order")
).show()
end = time.time()
print(f"{end - start :.2f}")
# ================================================================
# 完成
# ================================================================
print("\n===== Phase 2 Day 4-5 完成 =====")
print("检查清单：")
print("  [ ] 理解分区是什么，能看分区数、改分区数")
print("  [ ] 能区分窄依赖和宽依赖，知道哪些操作会触发 Shuffle")
print("  [ ] 理解 Shuffle 为什么慢，知道怎么减少 Shuffle 数据量")
print("  [ ] 能诊断数据倾斜（看 key 分布）")
print("  [ ] 能用加盐法修复数据倾斜")
print("  [ ] 完成综合挑战（电商订单场景）")
