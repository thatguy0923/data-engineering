#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 1 Day 13-14: 复习 + SQL 项目用 Pandas 重写
=================================================

目标：把 Phase 0 学的 SQL 操作全用 Pandas 重写一遍。
      ——你自己写，不是复制。

每一个关卡给了 SQL 写法，你的任务是在下面写出等价的 Pandas 代码。
写完后运行 python3 phase1_day13_sql_to_pandas.py，看结果对不对。
"""

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# 连接数据库
DB_URL = "mysql+pymysql://root:@127.0.0.1:3306/employees?charset=utf8mb4"
engine = create_engine(DB_URL)

# 加载数据
employees = pd.read_sql("employees", engine)
salaries = pd.read_sql("salaries", engine)
departments = pd.read_sql("departments", engine)
dept_emp = pd.read_sql("dept_emp", engine)
etl = pd.read_sql("etl_employees_clean_v2", engine)

print(f"已加载：employees={len(employees)} | salaries={len(salaries)} | etl={len(etl)}")


# ================================================================
# 第一关：GROUP BY + 聚合
# ================================================================
# SQL 版本（你要用 Pandas 写出等价结果）：
#
#   SELECT   department,
#            COUNT(*)      AS emp_count,
#            AVG(salary)   AS avg_salary,
#            MAX(salary)   AS max_salary
#   FROM     etl_employees_clean_v2
#   GROUP BY department
#   ORDER BY avg_salary DESC;

print("\n===== 第一关：GROUP BY =====")

# TODO: 用 Pandas 写出来
etl_grouped = etl.groupby("department").agg(
      emp_count=("emp_id", "size"),
      avg_salary=("salary", "mean"),
      max_salary=("salary", "max")
).sort_values("avg_salary", ascending=False).reset_index()
print(etl_grouped[["department", "emp_count", "avg_salary", "max_salary"]])

# ================================================================
# 第二关：CASE WHEN
# ================================================================
# SQL 版本：
#
#   SELECT emp_id, name, salary,
#       CASE
#           WHEN salary >= 15000 THEN '高薪'
#           WHEN salary >= 8000  THEN '中等'
#           ELSE '基础'
#       END AS level
#   FROM etl_employees_clean_v2;

print("\n===== 第二关：CASE WHEN =====")

# TODO: 用 np.select() 写出来
condition = [
    etl["salary"] >= 15000,
    etl["salary"] >= 8000
]
choices = ["高薪", "中等"]
etl["level"] = np.select(condition, choices, default = "基础")
print(etl[["emp_id", "name", "salary", "level"]])
# ================================================================
# 第三关：窗口函数 — 部门内排名（ROW_NUMBER）
# ================================================================
# SQL 版本：
#
#   SELECT emp_id, name, department, salary,
#       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank_in_dept
#   FROM etl_employees_clean_v2;

print("\n===== 第三关：ROW_NUMBER =====")

# TODO: 用 Pandas 的 rank() 写出来
etl["rank_in_dept"] = etl.groupby("department")["salary"].rank(method="first", ascending=False)

print(etl[["emp_id", "name", "department", "salary", "rank_in_dept"]])

# ================================================================
# 第四关：窗口函数 — 部门均值（AVG OVER）
# ================================================================
# SQL 版本：
#
#   SELECT emp_id, name, department, salary,
#       ROUND(AVG(salary) OVER (PARTITION BY department), 0) AS dept_avg
#   FROM etl_employees_clean_v2;

print("\n===== 第四关：AVG OVER =====")

# TODO: 用 transform() 写出来
etl["dept_avg"] = etl.groupby("department")["salary"].transform("mean").round(0)

print(etl[["emp_id", "name", "department", "salary", "dept_avg"]])

# ================================================================
# 第五关：INNER JOIN
# ================================================================
# SQL 版本：
#
#   SELECT e.emp_no, e.first_name, e.last_name, s.salary
#   FROM   employees e
#   JOIN   salaries s ON e.emp_no = s.emp_no;

print("\n===== 第五关：INNER JOIN =====")

# TODO: 用 pd.merge() 写出来
total = pd.merge(employees, salaries, on = "emp_no", how = "inner")
print(total[["emp_no", "first_name", "last_name", "salary"]])

# ================================================================
# 第六关：LEFT JOIN
# ================================================================
# SQL 版本：
#
#   SELECT e.emp_no, e.first_name, s.salary
#   FROM   employees e
#   LEFT JOIN salaries s ON e.emp_no = s.emp_no;

print("\n===== 第六关：LEFT JOIN =====")

# TODO: 用 pd.merge(how='left') 写出来
total_left = pd.merge(employees, salaries, on = "emp_no", how = "left")
print(total_left[["emp_no", "first_name", "salary"]])

# ================================================================
# 第七关：CTE（WITH ... AS）
# ================================================================
# SQL 版本：
#
#   WITH dept_avg AS (
#       SELECT department, AVG(salary) AS avg_sal
#       FROM   etl_employees_clean_v2
#       GROUP BY department
#   )
#   SELECT e.emp_id, e.name, e.department, e.salary, d.avg_sal
#   FROM   etl_employees_clean_v2 e
#   JOIN   dept_avg d ON e.department = d.department;

print("\n===== 第七关：CTE =====")

# TODO: 用中间 DataFrame 写出来
dept_avg = etl.groupby("department")["salary"].mean().reset_index().rename(columns={"salary": "avg_sal"})
result = pd.merge(etl, dept_avg, on = "department", how = "inner")
print(result[["emp_id", "name", "department", "salary", "avg_sal"]])

# ================================================================
# 第八关：综合项目 — 人才分析（用 Pandas 完整做一遍）
# ================================================================
# 之前你用 SQL 生成了 employee_talent_analysis 表。
# 现在用 Pandas 做同样的事：
#
#   employees + salaries + dept_emp + departments 四表 JOIN
#   → 算工作年限、薪资分位、人才标签
#   → 结果写入 MySQL 新表 pandas_talent_analysis_v2

print("\n===== 第八关：综合项目 =====")

# TODO: 自己写出完整的分析流程

# 1. 合并四张表
emp_sal = pd.merge(employees, salaries, on = "emp_no", how = "inner")
emp_sal_dept = pd.merge(emp_sal, dept_emp, on = "emp_no", how = "inner")
final = pd.merge(emp_sal_dept, departments, on = "dept_no", how = "inner")

# 2. 计算工作年限
final["work_years"] = pd.to_datetime("today").year - pd.to_datetime(final["hire_date"]).dt.year
#这里to_datetime（“today”），可以写为pd.Timestamp.now()效果一样，但是更加直观
# 3. 薪资百分位排名（在部门内）
conditions = [
      final["salary"] >= 15000,
      final["salary"] >= 8000,
]
choices = ["高薪", "中等"]
final["salary_level"] = np.select(conditions, choices, default = "基础")

# 4. 人才标签（高薪 + 资深 → 核心人才）
final["label"] = np.where((final["salary_level"] == "高薪") & (final["work_years"] >=10), "核心人才", "普通员工")

# 5. 输出统计
print("\n=== 人才分析结果 ===")
print(final[["emp_no", "first_name", "dept_name", "salary", "work_years", "salary_level", "label"]].head())

# 6. 写入 MySQL
final[["emp_no", "first_name", "dept_name", "salary", "work_years", "salary_level", "label"]].to_sql( #不取舍也可以直接final.to_sql
    "pandas_talent_analysis_v2",
    engine,
    if_exists="replace",
    index=False
)

print("\n===== 完成 =====")
