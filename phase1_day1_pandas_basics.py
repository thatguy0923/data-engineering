"""
============================================
Phase 1 Day 1: Pandas 基础 — SQL 翻译成 Python
============================================
每个操作都标注了对应 SQL 写法
"""

import pandas as pd

# ============================================
# 1. 加载数据 = FROM table
# ============================================
emp   = pd.read_csv('data_employees.csv')
sal   = pd.read_csv('data_salaries.csv')
dept  = pd.read_csv('data_departments.csv')
# de = pd.read_csv('data_dept_emp.csv')
# tit  = pd.read_csv('data_titles.csv')

# 看一眼数据 = SELECT * LIMIT 5
print("=== 员工表前5行 ===")
print(emp.head())

# 表信息 = DESCRIBE table
print("\n=== 薪资表统计 ===")
print(sal.describe())

# ============================================
# 练习 1: 仿照上面，加载 dept_emp 和 titles，看前5行
# ============================================

# 你的代码：
de = pd.read_csv('data_dept_emp.csv')
tit = pd.read_csv('data_titles.csv')
print("=== 部门员工表前5行 ===")
print(de.head())
print("=== 职位表前5行 ===")
print(tit.head())

# ============================================
# 2. 筛选行 = WHERE
# ============================================

# SQL: SELECT * FROM salaries WHERE salary > 30000 AND to_date = '9999-01-01'
high_sal = sal[(sal['salary'] > 30000) & (sal['to_date'] == '9999-01-01')]
print("=== 当前薪资 > 30000 ===")
print(high_sal)

# Pandas 符号对照：
#  SQL 的 AND →  &
#  SQL 的 OR  →  |
#  等于       →  ==
#  不等于     →  !=

# ============================================
# 练习 2: 筛选 engineering 部门的员工
# 提示：先看 departments 表是哪一行，然后 dept_emp 里 dept_no == 'd001'
# ============================================

# 你的代码：
de = pd.read_csv('data_dept_emp.csv')
eng_dept_emp = de[(de['dept_no'] == 'd001')]
print("=== engineering 部门员工 ===")
print(eng_dept_emp['emp_no'])

# ============================================
# 3. 排序 = ORDER BY
# ============================================

# SQL: SELECT * FROM salaries WHERE to_date='9999-01-01' ORDER BY salary DESC LIMIT 10
current_sal = sal[sal['to_date'] == '9999-01-01']     # WHERE
current_sal = current_sal.sort_values('salary', ascending=False)  # ORDER BY
print("=== 当前薪资 Top 10 ===")
print(current_sal.head(10))

# ============================================
# 练习 3: 列出员工表按 hire_date 从早到晚排，最早入职的 5 个人
# ============================================

# 你的代码：
early_hires = emp.sort_values('hire_date', ascending=True)
print("=== 最早入职的 5 个人 ===")
print(early_hires.head())

# ============================================
# 4. 选列 = SELECT 指定列
# ============================================

# SQL: SELECT emp_no, salary FROM salaries WHERE salary > 40000
result = sal[sal['salary'] > 40000][['emp_no', 'salary']]
print(result)

# ============================================
# 5. GROUP BY + 聚合 = GROUP BY + COUNT/AVG/SUM
# ============================================

# SQL: SELECT emp_no, COUNT(*) FROM salaries GROUP BY emp_no
raise_counts = sal.groupby('emp_no').size().reset_index(name='涨薪次数')
print("=== 每人涨薪次数 ===")
print(raise_counts.head(10))

# SQL: SELECT emp_no, AVG(salary) FROM salaries GROUP BY emp_no
avg_sal = sal.groupby('emp_no')['salary'].mean().reset_index()
avg_sal.columns = ['emp_no', '平均薪资']
# 取整
avg_sal['平均薪资'] = avg_sal['平均薪资'].round(0).astype(int)
print("\n=== 每人平均薪资 ===")
print(avg_sal.head(10))

# ============================================
# 练习 4: 算每个部门的平均薪资
# 提示：先 merge salaries 和 dept_emp，再 groupby dept_no
# ============================================

# 你的代码：
current_sal = sal[sal['to_date'] == '9999-01-01']
current_dept_emp = de[de['to_date'] == '9999-01-01']
dept_sal = pd.merge(current_sal, current_dept_emp, on='emp_no')
dept_avg_sal = dept_sal.groupby('dept_no')['salary'].mean().reset_index().rename(columns={'salary': 'avg_Salary'})
print("\n=== 每个部门的平均薪资 ===")
print(dept_avg_sal)

# ============================================
# 6. 写入文件 = 把结果存下来
# ============================================
raise_counts.to_csv('output_raise_counts.csv', index=False)
print("\n已保存 output_raise_counts.csv")

# ============================================
# 速查：SQL → Pandas 对照表
# ============================================
# SELECT col1, col2       →  df[['col1', 'col2']]
# WHERE col > 100         →  df[df['col'] > 100]
# AND / OR                →  & / |
# ORDER BY col DESC       →  df.sort_values('col', ascending=False)
# LIMIT 10                →  df.head(10)
# GROUP BY col            →  df.groupby('col')
# COUNT(*)                →  .size()
# AVG(col)                →  ['col'].mean()
# SUM(col)                →  ['col'].sum()
# AS 别名                 →  .rename(columns={'old': 'new'})
# ============================================