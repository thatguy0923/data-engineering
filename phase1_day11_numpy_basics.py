#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 1 Day 11-12: NumPy 基础 + 性能对比
==========================================

学习目标：
  1. NumPy 的核心数据结构：ndarray
  2. 向量化运算 vs Python 循环的性能差异
  3. NumPy 常用函数（数学/统计/布尔索引）
  4. 知道什么时候用 NumPy、什么时候用 Pandas、什么时候用纯 Python

为什么 NumPy 重要？
  - Pandas 底层就是 NumPy，你其实一直在用
  - PySpark 的向量化操作思路和 NumPy 一致
  - 处理百万级数据时，向量化 vs 循环能差 100 倍
"""

import time
import numpy as np

# =============================================================================
# 第 1 步：ndarray — NumPy 的核心
# =============================================================================
# 你之前用 Pandas 的 DataFrame 和 Series，它们的底层就是 ndarray。
# ndarray = n-dimensional array，多维数组。
# 和 Python 列表最大的区别：所有元素必须是同一类型，所以计算极快。

print("=" * 60)
print("一、ndarray 基础")
print("=" * 60)

# 从列表创建
arr = np.array([1, 2, 3, 4, 5])
print(f"从列表创建：{arr}")
print(f"类型：{type(arr)}")
print(f"元素 dtype：{arr.dtype}")       # int64 — 统一类型
print(f"形状：{arr.shape}")             # (5,) — 一维，5 个元素
print(f"维度数：{arr.ndim}")

# 创建特定数组
print(f"\n全零：{np.zeros(5)}")
print(f"全一：{np.ones(5)}")
print(f"0-9 序列：{np.arange(10)}")      # 类似 Python 的 range
print(f"等间距 10 个点：{np.linspace(0, 1, 10)}")  # 0 到 1 均匀取 10 个

# 二维数组
arr_2d = np.array([[1, 2, 3], [4, 5, 6]])
print(f"\n二维数组：\n{arr_2d}")
print(f"形状：{arr_2d.shape}")  # (2, 3) — 2 行 3 列

# =============================================================================
# 第 2 步：为什么不用 Python 列表？— 向量化运算
# =============================================================================
# Python 列表做运算：你得写 for 循环，每个元素一个个处理
# NumPy 数组做运算：一行代码，底层 C 一次处理全部

print("\n" + "=" * 60)
print("二、向量化 vs 循环 — 语法对比")
print("=" * 60)

# Python 列表：每个元素 +10，必须写循环
py_list = [1, 2, 3, 4, 5]
result = [x + 10 for x in py_list]   # 列表推导式，本质还是循环
print(f"Python 列表 +10：{result}")

# NumPy：一行
arr = np.array([1, 2, 3, 4, 5])
result = arr + 10                     # 向量化，每个元素同时 +10
print(f"NumPy 数组 +10：{result}")

# 数组之间的运算
a = np.array([1, 2, 3])
b = np.array([4, 5, 6])
print(f"\n数组相加：{a + b}")        # [5, 7, 9]
print(f"数组相乘：{a * b}")         # [4, 10, 18] — 逐元素乘，不是矩阵乘
print(f"开根号：{np.sqrt(a)}")      # [1.  1.414 1.732]

# 二维广播
matrix = np.array([[1, 2, 3], [4, 5, 6]])
print(f"\n原矩阵：\n{matrix}")
print(f"每列 + [10, 20, 30]：\n{matrix + [10, 20, 30]}")

# =============================================================================
# 第 3 步：性能实测 — 这就是 NumPy 存在的理由
# =============================================================================

print("\n" + "=" * 60)
print("三、性能对比：Python 循环 vs NumPy 向量化")
print("=" * 60)

SIZE = 1_000_000  # 100 万个数

# --- 纯 Python 循环 ---
py_list = list(range(SIZE))
start = time.time()
result = [x * 2 + 1 for x in py_list]
py_time = time.time() - start
print(f"Python 循环（{SIZE:,} 次运算）：{py_time:.4f} 秒")

# --- NumPy 向量化 ---
np_arr = np.arange(SIZE)
start = time.time()
result = np_arr * 2 + 1
np_time = time.time() - start
print(f"NumPy 向量化（{SIZE:,} 次运算）：{np_time:.4f} 秒")

speedup = py_time / np_time
print(f"\n🚀 NumPy 快 {speedup:.1f} 倍！")

# 测试多个操作
print("\n--- 更多对比 ---")

for size in [10_000, 100_000, 1_000_000]:
    py_data = list(range(size))
    np_data = np.arange(size)

    # 求和
    t1 = time.time()
    sum(py_data)
    t_py = time.time() - t1

    t1 = time.time()
    np.sum(np_data)
    t_np = time.time() - t1

    print(f"  {size:>10,} 个数求和：Python {t_py:.4f}s | NumPy {t_np:.4f}s | 快 {t_py/t_np:.0f}x")

# =============================================================================
# 第 4 步：NumPy 常用函数
# =============================================================================

print("\n" + "=" * 60)
print("四、NumPy 常用函数")
print("=" * 60)

arr = np.array([15, 3, 8, 22, 7, 10, 18, 5])

# ---- 统计 ----
print(f"原数组：{arr}")
print(f"总和：{arr.sum()} | 均值：{arr.mean():.2f} | 中位数：{np.median(arr)}")
print(f"标准差：{arr.std():.2f} | 方差：{arr.var():.2f}")
print(f"最小值：{arr.min()} | 最大值：{arr.max()}")
print(f"最小值位置：{arr.argmin()} | 最大值位置：{arr.argmax()}")

# ---- 排序 ----
print(f"\n排序：{np.sort(arr)}")
print(f"原数组不变：{arr}")  # np.sort 不修改原数组

# ---- 布尔索引（类似 Pandas 的 df[df['salary'] > 10000]）----
print(f"\n大于 10 的元素：{arr[arr > 10]}")
print(f"偶数：{arr[arr % 2 == 0]}")
print(f"大于 5 且小于 15：{arr[(arr > 5) & (arr < 15)]}")  # 注意用 & 不是 and

# ---- 重塑 ----
arr_1d = np.arange(12)
arr_reshaped = arr_1d.reshape(3, 4)       # 变 3 行 4 列
print(f"\n重塑 12 元素 → 3×4：\n{arr_reshaped}")

# ---- 随机数 ----
np.random.seed(42)  # 固定随机种子，结果可复现
print(f"\n均匀分布 (0-1)：{np.random.rand(5)}")
print(f"正态分布 (μ=0, σ=1)：{np.random.randn(5)}")
print(f"随机整数 (0-100)：{np.random.randint(0, 100, 10)}")

# =============================================================================
# 第 5 步：NumPy 在数据工程里的实际用途
# =============================================================================

print("\n" + "=" * 60)
print("五、数据工程中的 NumPy 场景")
print("=" * 60)

# ---- 场景 1：生成模拟数据 ----
print("【场景 1】生成 10000 条员工模拟数据")
n = 10000
np.random.seed(42)

salaries = np.random.normal(8000, 3000, n).astype(int)  # 均 8000，标准差 3000
salaries = np.clip(salaries, 3000, 50000)               # 限制范围

bonus = np.random.normal(2000, 800, n).astype(int)
bonus = np.clip(bonus, 0, 10000)

total = salaries + bonus  # 向量化加法
print(f"生成 {n:,} 条数据:")
print(f"  底薪：均值 {salaries.mean():.0f}，中位数 {np.median(salaries):.0f}")
print(f"  奖金：均值 {bonus.mean():.0f}，中位数 {np.median(bonus):.0f}")
print(f"  总收入：均值 {total.mean():.0f}，中位数 {np.median(total):.0f}")

# ---- 场景 2：异常检测 ----
print("\n【场景 2】Z-score 异常检测")
z_scores = np.abs((salaries - salaries.mean()) / salaries.std())
outliers = z_scores > 3
print(f"Z-score > 3 的异常薪资：{outliers.sum()} 条 ({outliers.sum()/len(salaries)*100:.1f}%)")
print(f"异常值示例：{salaries[outliers][:5]}")

# ---- 场景 3：分箱/分桶 ----
print("\n【场景 3】薪资分桶统计")
bins = [0, 5000, 10000, 15000, 20000, 100000]
labels = ["<5k", "5-10k", "10-15k", "15-20k", ">20k"]
# 用 Pandas 也行，但 NumPy 可以直接操作
digitized = np.digitize(salaries, bins)
for i, label in enumerate(labels, 1):
    count = (digitized == i).sum()
    print(f"  {label}: {count} 人 ({count/len(salaries)*100:.1f}%)")

# ---- 场景 4：滚动窗口计算 ----
print("\n【场景 4】滚动均值（最近 7 天均薪）")
# 假设这是过去 30 天的日薪数据
daily_income = np.random.normal(300, 50, 30)
window = 7
# 手动算滚动均值（不用 Pandas rolling）
rolling_mean = np.array([
    daily_income[i:i+window].mean()
    for i in range(len(daily_income) - window + 1)
])
print(f"   30 天日薪 → 24 天滚动均值")
print(f"   前 3 个窗口：{np.round(rolling_mean[:3], 1)}")

# =============================================================================
# 第 6 步：三个工具的分工 — 什么时候用什么
# =============================================================================

print("\n" + "=" * 60)
print("六、Python 列表 / NumPy / Pandas — 选谁？")
print("=" * 60)

print("""
┌─────────────────┬──────────────────────────┬───────────────────────────┐
│ 工具             │ 什么时候用                │ 例子                       │
├─────────────────┼──────────────────────────┼───────────────────────────┤
│ Python 列表      │ 少量杂数据、非数值        │ 存几个名字、配置列表       │
│ NumPy ndarray    │ 纯数值、大量计算、矩阵     │ 数学运算、ML、图像处理    │
│ Pandas DataFrame │ 表格数据、混合类型列       │ CSV/数据库读写、清洗、ETL │
└─────────────────┴──────────────────────────┴───────────────────────────┘
""")

print("结论：")
print("  • 你日常 ETL → Pandas（表格操作方便）")
print("  • 遇到百万行以上的数值计算 → 把 DataFrame 的列转 NumPy 再算")
print("  • 纯 Python 循环 → 除了读配置/小列表，数据工程里尽量别用")

# 实际操作：Pandas 取列 → NumPy 算
import pandas as pd

df = pd.DataFrame({
    "salary": salaries[:1000],
    "bonus": bonus[:1000],
})
# Pandas 列直接就是 NumPy 数组的封装
salary_array = df["salary"].values  # 拿到 ndarray
total_array = salary_array + df["bonus"].values
print(f"\n验证：Pandas 列 .values 就是 ndarray → {type(salary_array)}")

# =============================================================================
# 核心知识点总结
# =============================================================================
"""
┌──────────────────────┬──────────────────────────────────────────────────────┐
│ 概念                  │ 一句话                                                │
├──────────────────────┼──────────────────────────────────────────────────────┤
│ ndarray              │ NumPy 的核心数据结构，同类型多维数组，C 底层实现       │
│ 向量化运算            │ 一行操作整个数组，不用写循环，比 Python 循环快 10-100 倍 │
│ dtype                │ 数组元素的统一类型（int64/float64），统一才能快        │
│ 广播 (broadcasting)   │ 不同形状的数组自动对齐运算，NumPy 最聪明的设计         │
│ 布尔索引              │ arr[arr > 10]，和 Pandas 的 df[df['col'] > 10] 同源  │
│ 随机数                │ 生成模拟数据的利器，数据工程面试经常让你造测试数据      │
│ Z-score              │ (值-均值)/标准差，>3 算异常，数据清洗常用               │
└──────────────────────┴──────────────────────────────────────────────────────┘

面试可能问到：
  Q: NumPy 为什么比 Python 循环快？
  A: ① 统一类型，内存连续，CPU 缓存友好
     ② 底层用 C/Fortran 写好的向量化操作，一次处理整个数组
     ③ Python 循环每次迭代都有解释器开销（类型检查、引用计数等）
     ④ （加分项）NumPy 可以利用 SIMD 指令，一条指令算多个数

  Q: DataFrame 和 ndarray 什么关系？
  A: DataFrame 的每一列底层就是一个 ndarray（或扩展的 ExtensionArray）。
     可以用 df['col'].values 拿到 ndarray，用 NumPy 函数直接计算。

  Q: 什么时候 Pandas 不如直接 NumPy？
  A: 纯数值矩阵运算、大量线性代数计算时 NumPy 更快且代码更简洁。
     但日常 ETL 还是 Pandas，因为它处理混合类型、缺失值、列标签更方便。
"""

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Day 11-12 完成 🎉")
    print("=" * 60)
    print(f"\n学习检查清单：")
    print(f"  [ ] ndarray 的创建和基本属性")
    print(f"  [ ] 向量化 vs 循环 — 语法和性能")
    print(f"  [ ] 常用统计/排序/布尔索引")
    print(f"  [ ] NumPy 的随机数生成")
    print(f"  [ ] 知道什么时候选 NumPy / Pandas / 纯 Python")
