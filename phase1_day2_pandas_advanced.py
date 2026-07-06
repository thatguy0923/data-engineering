"""
============================================
Phase 1 Day 2: Pandas 进阶 — JOIN/透视/缺失值
============================================
对应 SQL 的 JOIN、GROUP BY 多维聚合、数据清洗
"""

import pandas as pd

# 加载数据
emp  = pd.read_csv('data_employees.csv')
sal  = pd.read_csv('data_salaries.csv')
de   = pd.read_csv('data_dept_emp.csv')
dep  = pd.read_csv('data_departments.csv')
tit  = pd.read_csv('data_titles.csv')

# ============================================
# 1. MERGE = JOIN
# ============================================

# SQL: SELECT * FROM salaries s JOIN dept_emp de ON s.emp_no = de.emp_no
merged = pd.merge(sal, de, on='emp_no')
print("=== sal JOIN dept_emp ===")
print(merged.head())

# 多列 JOIN：ON a.col1 = b.col1 AND a.col2 = b.col2
merged2 = pd.merge(sal, de, on=['emp_no', 'to_date'])
print("\n=== 双列 JOIN ===")
print(merged2.head())

# LEFT JOIN：keep all rows from left table
merged_left = pd.merge(sal, de, on='emp_no', how='left')
print(f"\nsal rows: {len(sal)}, LEFT JOIN rows: {len(merged_left)}")

# ============================================
# 练习 1: 三表 JOIN — 员工姓名 + 当前薪资 + 部门名称
# 提示：emp JOIN sal ON emp_no → 结果再 JOIN dep ON dept_no
#       都只取 to_date='9999-01-01'
# ============================================

# 你的代码：
merged_emp_sal = pd.merge(de[de['to_date'] == '9999-01-01'], sal[sal['to_date'] == '9999-01-01'], on='emp_no')
merged_emp_sal_dept = pd.merge(merged_emp_sal, dep, on='dept_no')
print("\n=== 员工姓名 + 当前薪资 + 部门名称 ===")
print(merged_emp_sal_dept[['emp_no', 'salary', 'dept_name']])

# ============================================
# 2. 透视表 = GROUP BY + 行列转置
# ============================================

# SQL: 按部门和年份统计平均薪资，部门当行，年份当列
sal_current = sal[sal['to_date'] == '9999-01-01']
dept_sal = pd.merge(sal_current, de[de['to_date'] == '9999-01-01'], on='emp_no')

# pivot_table：行=dept_no，值=salary，聚合=mean
pivot = dept_sal.pivot_table(
    values='salary',
    index='dept_no',
    aggfunc='mean'
)
print("\n=== 部门平均薪资透视 ===")
print(pivot)

# ============================================
# 练习 2: 按性别统计每个部门的平均薪资
# 提示：先 JOIN emp 拿到 gender → pivot_table(index='dept_no', columns='gender', values='salary')
# ============================================

# 你的代码：
pivot_gender = pd.merge(emp, sal[sal['to_date'] == '9999-01-01'], on = 'emp_no')
pivot_gender = pd.merge(pivot_gender, de[de['to_date'] == '9999-01-01'], on = 'emp_no').pivot_table(
    index='dept_no',
    columns='gender',
    values='salary',
    aggfunc='mean'
).round(0)
print("\n=== 按性别统计每个部门的平均薪资 ===")
print(pivot_gender)

# ============================================
# 3. 缺失值处理
# ============================================

# 造一些缺失数据
import numpy as np
df = emp.copy()
df.loc[0, 'first_name'] = None     # 第一行名字变空
df.loc[3, 'birth_date'] = None     # 第四行生日变空
df.loc[5:8, 'hire_date'] = None    # 第6~9行入职日期变空
print("=== 缺失数据 ===")
print(df.head(10))

# 检查缺失
print("\n=== 每列缺失数 ===")
print(df.isna().sum())

# 填缺失：字符串填 '未知'，日期填默认值
df['first_name'] = df['first_name'].fillna('未知')
df['birth_date'] = df['birth_date'].fillna('1900-01-01')
# df.dropna()  # 或者直接删掉有缺失的行

print("\n=== 填补后 ===")
print(df.head(10))

# ============================================
# 练习 3: 检查 salaries 表里有没有缺失值
# ============================================

# 你的代码：
print("\n=== salaries 表每列缺失数 ===")
print(sal.isna().sum())

# ============================================
# 4. 多层 groupby + 多聚合
# ============================================

# SQL: SELECT dept_no, emp_no, COUNT(*), AVG(salary), MAX(salary)
#      FROM salaries JOIN dept_emp ... GROUP BY dept_no, emp_no
agg = dept_sal.groupby('dept_no').agg(
    人数=('emp_no', 'count'),
    平均薪资=('salary', 'mean'),
    最高薪资=('salary', 'max'),
    最低薪资=('salary', 'min')
).reset_index()

# 取整
agg['平均薪资'] = agg['平均薪资'].round(0).astype(int)
print("\n=== 部门薪资报告 ===")
print(agg)

# ============================================
# 练习 4: 把部门薪资报告保存为 CSV
# ============================================

# 你的代码：
agg.to_csv('output_dept_salary_report.csv', index=True)

# ============================================
# Day 2 速查
# ============================================
# MERGE              →  pd.merge(df1, df2, on='col', how='left')
# 透视表              →  df.pivot_table(values='', index='', columns='', aggfunc='')
# 查缺失              →  df.isna().sum()
# 填缺失              →  df['col'].fillna('替代值')
# 删缺失行            →  df.dropna()
# 多聚合              →  df.groupby('col').agg(新名=('列', '函数'))
# ============================================
