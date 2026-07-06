"""
================================================================
Phase 1 Day 3-4: 脏数据清洗完整流程（详细解析版）
================================================================

核心思路：
  现实中的数据永远是脏的。数据工程师 80% 的时间在洗数据，20% 在分析。
  这一节走完「发现脏数据 → 判断怎么修 → 动手修 → 验证结果」的完整链路。

你会学到：
  - 如何系统性地给数据做"体检"
  - 缺失值的 4 种处理策略（删/填固定值/填统计值/分组填）
  - 重复值、异常值、格式错乱的处理
  - groupby + transform 的配合（面试高频考点）
  - pivot_table（透视表）和 melt（逆透视）的实战用法
"""

import pandas as pd
import numpy as np

# ╔══════════════════════════════════════════════════════════════╗
# ║              第一步：加载数据                                  ║
# ╚══════════════════════════════════════════════════════════════╝
#
# pd.read_csv() 是 Pandas 最常用的读文件函数。
# 它背后做的事：
#   1. 按行读取文本文件
#   2. 第一行默认当列名（header=0）
#   3. 逗号当分隔符（sep=","）
#   4. 自动推断每列的数据类型（dtype）
#      - 全是数字 → int64 或 float64
#      - 有文字 → object（就是 Python 的 str）
#
# .shape 返回 (行数, 列数)，是最快的"看一眼数据有多大规模"的方法。
# .head(n) 看前 n 行，快速感受数据长什么样。

print("=" * 60)
print("第一步：加载数据")
print("=" * 60)

df = pd.read_csv("data_employees_dirty.csv")
print(f"原始数据: {df.shape[0]} 行 × {df.shape[1]} 列")
print(df.head(10))   # 扫一眼前10行，心里有数
print()

# ╔══════════════════════════════════════════════════════════════╗
# ║              第二步：数据体检（最重要的一步）                    ║
# ╚══════════════════════════════════════════════════════════════╝
#
# 原则：先看清楚问题在哪，再动手修。不看就修 = 瞎修。
#
# 标准体检清单（每拿到一张新表都这么过一遍）：
#   ① 每列是什么类型？（数字/文字/日期？）
#   ② 哪些列有空值？空了多少？
#   ③ 有没有完全重复的行？
#   ④ 数字列的范围合理吗？（有没有负数薪资？有没有离谱的值？）
#   ⑤ 文字列的格式统一吗？（日期、手机号、地址）
#   ⑥ 分类列的值合理吗？（有没有不该出现的类别？）

print("=" * 60)
print("第二步：数据体检 — 找出所有毛病")
print("=" * 60)


# --- 体检①：列类型 ---
# .dtypes 显示每列的 Pandas 数据类型：
#   int64    = 整数
#   float64  = 小数（注意：有 NaN 的整数列也会变 float，因为 NaN 只有 float 支持）
#   object   = 字符串/混合类型（最通用的，但性能最差）
#
# 为什么看这个？
#   比如 salary 应该是数字，如果它是 object，说明里面有非数字的脏东西混进去了。
#   hire_date 应该是日期但现在是 object，说明格式不统一，需要转。

print("\n--- 体检①：每列类型 ---")
print(df.dtypes)


# --- 体检②：缺失值 ---
# .isnull() 返回 True/False 的 DataFrame（每个格子：是空=True，不空=False）
# .sum() 对每列求和 → True=1, False=0 → 得到每列的空值个数
#
# 缺失值的本质：
#   - CSV 里连续两个逗号 ,, 夹出来的空位
#   - 或者写了 "null"、"NA"、"N/A"、空字符串
#   - Pandas 统一读成 NaN（Not a Number）
#   - NaN 有一个诡异特性：NaN != NaN（自己不等于自己，Python 里唯一一个）

print("\n--- 体检②：缺失值统计 ---")
print(df.isnull().sum())


# --- 体检③：重复行 ---
# .duplicated() 判断每一行是否和前面的某行完全一样
#   keep=False: 标记所有重复行（第一次出现也标记），方便我们看清"谁跟谁重复了"
#   keep='first': 只标记后面重复的，保留第一次出现（默认行为）
#   keep='last':  只标记前面重复的，保留最后一次出现
#
# 重复行可能是：
#   - 数据录入时同一个人被登记了两次
#   - 系统导出时日志重复推送
#   - ETL 任务跑了两次

print(f"\n--- 体检③：重复行: {df.duplicated().sum()} 条 ---")

# dup_mask 是一个布尔 Series，True 的那行就是重复的
dup_mask = df.duplicated(keep=False)
if dup_mask.sum() > 0:
    print(df[dup_mask].sort_values("emp_id"))   # 按 emp_id 排序，把重复的放一起看


# --- 体检④：数值列统计 ---
# .describe() 是 Pandas 最快的"数字列概览"：
#   count = 非空个数
#   mean  = 平均值
#   std   = 标准差（衡量数据分散程度）
#   min   = 最小值 → 看有没有负数或 0
#   25%   = 第一四分位数
#   50%   = 中位数（和 mean 差距大 → 数据偏斜，有极端值）
#   75%   = 第三四分位数
#   max   = 最大值 → 看有没有离谱的异常值
#
# 举例：这里 max=200000，mean=22500，说明有一个超高的值把平均值拉上去了。
# 真实企业里，薪资 20 万可能是高管，不一定是错误，但要标记出来。

print("\n--- 体检④：薪资统计 ---")
print(df["salary"].describe())


# --- 体检⑤：日期格式检查 ---
# 日期格式不一致是数据清洗的经典难题。同一个文件里可能出现：
#   2020-03-15（标准）
#   2020/06/01（斜杠）
#   2021-1-1（缺前导零）
#   2023-13-01（不存在的月份）
#   2020-02-30（不存在的日期）
#
# .dropna() 先把空值丢掉（不然没法看格式），然后抽样看前几条

print("\n--- 体检⑤：日期格式抽样 ---")
print(df["hire_date"].dropna().head(10))


# --- 体检⑥：文字列格式检查（手机号）---
# str.match() 是 Pandas 的字符串正则匹配方法
#   正则 r"^1\d{10}$" 的含义：
#     ^      = 字符串开头
#     1      = 必须以 1 开头（中国大陆手机号）
#     \d{10} = 后面跟 10 个数字（\d = 数字, {10} = 正好10个）
#     $      = 字符串结尾（不能有多余字符）
#   所以这个正则在说："正好 11 位，以 1 开头，全是数字"
#   ～是“非”
# na=False：如果这格是 NaN，match 结果当 False 处理（否则会报错）

print("\n--- 体检⑥：手机号异常（格式不对的）---")
bad_phone = df["phone"].notna() & ~df["phone"].str.match(r"^1\d{10}$", na=False)
if bad_phone.sum() > 0:
    print(df[bad_phone])


# --- 体检⑦：异常值——薪资不合理 ---
# 条件: 小于等于 0（负数或 0 薪资） 或 大于 50000（远超正常范围）
# | 是 Pandas 的"或"运算符（不能用 Python 的 or，那是单个 bool 用的）
# & 是 Pandas 的"且"运算符（不能用 Python 的 and）
#
# 注意括号！每个条件必须单独括起来，因为 & | 的优先级比比较运算符诡异

print("\n--- 体检⑦：薪资异常（负数或超标）---")
print(df[(df["salary"] <= 0) | (df["salary"] > 50000)])


# ╔══════════════════════════════════════════════════════════════╗
# ║              第三步：逐列清洗（核心）                           ║
# ╚══════════════════════════════════════════════════════════════╝
#
# 清洗缺失值有 4 种策略，按场景选择：
#
#   策略              | 什么时候用              | 例子
#   ─────────────────────────────────────────────────────────
#   ① 删掉那行        | 缺失很少 + 不影响分析   | dropna()
#   ② 填固定值        | 有业务含义的缺失        | 部门→"待分配"
#   ③ 填统计值        | 数值列，不想丢样本      | 薪资→中位数
#   ④ 分组填统计值    | 组内更准确              | 薪资→同部门中位数
#
#   ⚠️ 中位数 vs 平均值：有异常值时中位数更稳。
#      比如工程部有一个 20 万的高管，用平均值会被拉高，中位数不受影响。

print("\n" + "=" * 60)
print("第三步：逐列清洗")
print("=" * 60)

# ── 3.1 去重 ──
# .drop_duplicates() 删掉完全重复的行（所有列值都一样才算重复）
# 如果想按某列去重（比如 emp_id 重复就认为重复），用 subset=['emp_id']
# keep='first' 是默认：保留第一次出现的，删后面重复的

before = len(df)
df = df.drop_duplicates()
print(f"1. 去重: {before} → {len(df)} ({before - len(df)} 条)")

# ── 3.2 姓名缺失 ──
# fillna() 是最基础的填缺失函数
# 这里用字符串拼接： "员工_" + emp_id → "员工_1005"
# .astype(str): emp_id 是 int，要转成 str 才能用 + 拼接
#
# 实际工作中，姓名缺失更可能是身份证/工号匹配补上，这里造不出那么多数据，用 ID 代替

df["name"] = df["name"].fillna("员工_" + df["emp_id"].astype(str))
print("2. 姓名缺失 → 已用 emp_id 填充")

# ── 3.3 部门缺失 ──
# 部门缺失标为"待分配"，这是有业务含义的固定值填充
# 为什么不用中位数？部门是分类变量（category），没有"中位数"这个概念
# → 分类变量缺失：填固定值或众数（mode）

df["department"] = df["department"].fillna("待分配")
print("3. 部门缺失 → 已标为'待分配'")

# ── 3.4 薪资缺失：分组中位数填充 ──
# 这一段是面试高频考点，值得拆开细讲：
#
# 第一层：df.groupby("department")["salary"]
#   → 按部门分组，每组只看 salary 这一列
#
# 第二层：.transform(lambda x: x.fillna(x.median()))
#   → transform 是"每组内做同样的事，但返回跟原数据一样长的结果"
#   → 大白话：你在哪个组，就用哪个组的中位数填你的空
#   → transform 和 agg 的区别（必考）：
#       agg()  : 每组返回一个值   → 结果长度 = 组数
#       transform(): 每组返回跟组员一样多的值 → 结果长度 = 原数据行数
#
#   📝 举例对比：
#       groupby("部门")["薪资"].agg("median")  → 5 行（5 个部门）
#       groupby("部门")["薪资"].transform("median") → 18 行（每个员工都有）
#
# 第三层：全局 df["salary"].fillna(df["salary"].median())
#   → 兜底：如果某部门所有人薪资都缺失（median 算出来是 NaN），用全局中位数

df["salary"] = df.groupby("department")["salary"].transform(
    lambda x: x.fillna(x.median())
)
df["salary"] = df["salary"].fillna(df["salary"].median())
print("4. 薪资缺失 → 已用同部门中位数填充")

# ── 3.5 负数薪资 ──
# .loc[条件, 列名] 是 Pandas 的"定位修改"语法：
#   行选择: df["salary"] < 0   → 布尔索引，找出负数薪资的行
#   列选择: "salary"           → 只改这一列
#   赋新值: abs(...)           → Python 内置函数，取绝对值
#
# 为什么用 .loc 而不是直接 df[df["salary"]<0]["salary"]？
#   → 后者会返回一个副本（copy），改了不影响原数据 → 会出现 SettingWithCopyWarning
#   → .loc 直接操作原数据，安全且高效

neg_count = (df["salary"] < 0).sum()
df.loc[df["salary"] < 0, "salary"] = abs(df.loc[df["salary"] < 0, "salary"])
print(f"5. 负数薪资 → {neg_count} 条已取绝对值")

# ── 3.6 手机号清洗 ──
# 两步处理：
#   Step 1: 空值 → "未知"（NaN 是 float，填成字符串后整列变成 object）
#   Step 2: 不是空也不是有效手机号的 → "无效号码"
#
# 正则 r"^1\d{10}$" 再次使用:
#   1[3-9]\d{9} 更严格（匹配真实号段），但这里用简版

df["phone"] = df["phone"].fillna("未知")
invalid_phone = (df["phone"] != "未知") & ~df["phone"].str.match(r"^1\d{10}$", na=False)
df.loc[invalid_phone, "phone"] = "无效号码"
print(f"6. 手机号 → 空值标未知，格式错的标无效")

# ── 3.7 日期统一 ──
# pd.to_datetime() 是 Pandas 的日期解析器，非常聪明但也很挑剔：
#   能自动识别: "2020-03-15", "2020/03/15", "2020.03.15", "15-Mar-2020"
#   不能识别: "2021-1-1"（缺前导零有时行有时不行）、"2023-13-01"（13 月不存在）
#   errors="coerce": 解析不了的变成 NaT（Not a Time，日期的 NaN）
#
# .apply(func): 对 Series 的每个值执行 func，返回新的 Series
#   这跟 Python 内置的 map() 一样，但 .apply() 是 Pandas 的，支持更多特性

def clean_date(date_str):
    """
    把各种乱七八糟的日期统一成 YYYY-MM-DD 格式
    """
    if pd.isna(date_str):           # 如果是 NaN，直接返回 NaT（Not a Time）
        return pd.NaT
    date_str = str(date_str).replace("/", "-")   # 统一分隔符：斜杠→横杠
    try:
        # pd.to_datetime 会自动处理各种合法日期，返回 Timestamp 对象
        # .strftime("%Y-%m-%d") 格式化成标准字符串
        return pd.to_datetime(date_str).strftime("%Y-%m-%d")
    except:
        return pd.NaT                 # 解析失败 = 无效日期（如 2月30日、13月）

df["hire_date"] = df["hire_date"].apply(clean_date)
bad_dates = df["hire_date"].isna().sum()
print(f"7. 日期标准化完成，{bad_dates} 条无效日期标为空")

# ── 3.8 经理缺失 ──
# 思路：同一部门的人通常对应同一个经理，所以用"同部门最常见的经理"填充
#
# x.mode() 返回众数（出现次数最多的值）：
#   - 如果有多个并列，返回所有并列值 → 取第一个 [0]
#   - 如果全为空，返回空 Series → .empty 为 True → 填"未知"
#
# 这道题本质上是：分类变量的缺失，怎么用组内信息补全。

df["manager"] = df.groupby("department")["manager"].transform(
    lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "未知")
)
print("8. 经理缺失 → 已用同部门最常见经理填充")


# ╔══════════════════════════════════════════════════════════════╗
# ║         第四步：验证清洗结果 + 导出                             ║
# ╚══════════════════════════════════════════════════════════════╝
#
# 洗完数据后必须验证：缺失值是否都填了？数值范围是否合理了？
# 这一步就是"质量检查"，现实工作中可能会写自动化脚本做这个。
#
# .to_csv() 把 DataFrame 写入 CSV 文件
#   index=False: 不要写行号（否则 CSV 第一列会是 0,1,2,3...）
#   如果要压缩: .to_csv("xxx.csv.gz", compression="gzip")

print("\n" + "=" * 60)
print("第四步：清洗结果验证")
print("=" * 60)

print(f"\n最终数据: {df.shape[0]} 行 × {df.shape[1]} 列")

print("\n--- 缺失值检查（应该只剩无效日期是空的）---")
print(df.isnull().sum())

print("\n--- 各部门人数 ---")
print(df["department"].value_counts())

print("\n--- 薪资分布 ---")
print(df["salary"].describe())

# 导出
df.to_csv("data_employees_clean.csv", index=False)
print("\n✅ 已导出: data_employees_clean.csv")


# ╔══════════════════════════════════════════════════════════════╗
# ║       第五步：清洗后的分析 — merge / pivot / melt              ║
# ╚══════════════════════════════════════════════════════════════╝
#
# 数据干净了，才能做分析。这一步展示三个 Pandas 核心操作：
#   - groupby().agg():  分组聚合（对标 SQL 的 GROUP BY）
#   - pivot_table():    数据透视表（对标 Excel 的透视表）
#   - melt():           宽表 → 长表（逆透视）
#
# pivot_table vs pivot 的区别：
#   - pivot():  简单重塑，不聚合。要求行列组合唯一，重复会报错
#   - pivot_table(): 带聚合的重塑。行列重复时用 aggfunc 合并（默认求均值）
#   → 实际工作 99% 用 pivot_table，因为数据很少有完美唯一的行列组合

print("\n" + "=" * 60)
print("第五步：用清洗好的数据做分析")
print("=" * 60)

# 复制一份干净的，并把日期转成真正的 datetime 类型（方便后续按年月分析）
clean = df.copy()
clean["hire_date"] = pd.to_datetime(clean["hire_date"], errors="coerce")

# ── 5.1 分组聚合：各部门薪资汇总 ──
# .groupby("分组列").agg({列名: 聚合函数, ...})
# 或者用新语法: .agg(新列名=("原列", "函数"))   ← 更直观
#
# 聚合函数可以是: "mean","sum","count","max","min","std","median"
# 也可以是自定义 lambda
#
# .round(0): 四舍五入到整数（薪资不需要小数点）

print("\n--- 各部门薪资汇总（对标 GROUP BY）---")
dept_stats = clean.groupby("department").agg(
    人数=("emp_id", "count"),
    平均薪资=("salary", "mean"),
    最高薪资=("salary", "max"),
    最低薪资=("salary", "min"),
).round(0)
print(dept_stats)


# ── 5.2 PIVOT TABLE：部门 × 经理 的薪资透视 ──
# .pivot_table() 的参数：
#   values="salary"   → 要看的数值
#   index="department" → 行标签（Excel 透视表的"行"）
#   columns="manager"  → 列标签（Excel 透视表的"列"）
#   aggfunc="mean"    → 交叉处的值怎么算（默认就是 mean）
#
# 这张表的读法：
#   行=Engineering, 列=王经理 → 41179.0
#   意思是：工程部 + 王经理管辖 → 平均薪资 41179
#   NaN 表示这个交叉组合没有数据（比如 Data 部门没有王经理管的人）

print("\n--- 透视表：各部门 × 各经理 平均薪资 ---")
pivot = clean.pivot_table(
    values="salary",
    index="department",
    columns="manager",
    aggfunc="mean"
).round(0)
print(pivot)


# ── 5.3 MELT：宽表拉回长表 ──
# pd.melt() 是 pivot_table 的反向操作：
#   宽表（很多列） → 长表（属性名变成一列，属性值变成一列）
#
# 参数解析：
#   id_vars=["emp_id", "name"]:
#     → 这些列不动，每行保留原值（相当于"身份证"列）
#   value_vars=["department", "salary"]:
#     → 这两列要被"熔化"掉：列名变成"属性"列的值，列值变成"值"列的值
#   var_name="属性"
#     → 原来列名放哪列？起个名字叫"属性"
#   value_name="值"
#     → 原来单元格的值放哪列？起个名字叫"值"
#
# 为什么需要 melt？
#   很多可视化库（seaborn、plotly）要求长格式数据
#   数据建模时，长表是规范化的（符合数据库第三范式）

print("\n--- Melt：宽表拉回长表 ---")
melted = pd.melt(
    clean,
    id_vars=["emp_id", "name"],
    value_vars=["department", "salary"],
    var_name="属性",
    value_name="值"
)
print(melted.head(10))

print("\n" + "=" * 60)
print("✅ Day 3-4 完成！")
print("=" * 60)
print(f"""
┌─────────────────────────────────────────────────────────┐
│  总结：你今天掌握了                                       │
│                                                         │
│  🩺 数据体检 6 步法                                      │
│     dtypes → isnull → duplicated → describe             │
│     → 日期/手机号格式检查 → 异常值筛查                     │
│                                                         │
│  🧹 缺失值 4 种策略                                       │
│     固定值 / 统计值 / 分组统计 / 删除                      │
│                                                         │
│  🔧 关键方法                                             │
│     groupby().transform() ← 面试必问                      │
│     .loc[条件, 列] = 值    ← 安全修改数据                  │
│     .apply(自定义函数)      ← 万能转换                     │
│     .pivot_table()         ← 透视表                      │
│     .melt()                ← 宽表拉长                     │
│                                                         │
│  清洗结果: data_employees_clean.csv                       │
└─────────────────────────────────────────────────────────┘
""")
