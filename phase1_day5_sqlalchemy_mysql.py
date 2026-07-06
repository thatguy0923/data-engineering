#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 1 Day 5-6: SQLAlchemy + Pandas 连 MySQL 读写字
====================================================

学习目标：
  1. 用 SQLAlchemy 创建引擎，连接本地 MySQL
  2. pd.read_sql() 把 SQL 查询结果直接变成 DataFrame
  3. df.to_sql() 把 DataFrame 写回 MySQL 表
  4. 组合 pandas 清洗 + SQLAlchemy 入库，形成完整流水线

环境：
  - MySQL 9.6.0 (Homebrew)，用户 root，无密码
  - 数据库 employees（5 张表：employees, salaries, departments, dept_emp, titles）
  - sqlalchemy 2.0.50 + pymysql 2.2.8 + pandas 3.0.3

为什么这很重要？
  实际工作中数据很少是干净的 CSV——大部分存在数据库里。
  Python 负责"算"，SQL 负责"存和查"，两个配合才是真实工作流。
"""

import pandas as pd
from sqlalchemy import create_engine, text
import pymysql

# =============================================================================
# 第 1 步：创建数据库连接
# =============================================================================
# SQLAlchemy 的连接格式：
#   mysql+pymysql://用户名:密码@主机:端口/数据库名?charset=utf8mb4
#
# create_engine() 只是创建引擎对象，还没真正连接。
# 真正的连接在第一次执行 SQL 时才建立（惰性连接）。

DB_CONFIG = {
    "user": "root",
    "password": "",           # 本地开发没设密码
    "host": "127.0.0.1",      # 127.0.0.1 ≠ localhost（前者走 TCP，后者走 socket）
    "port": 3306,
    "database": "employees",
}

# f-string 拼出连接字符串
CONN_STR = (
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    f"?charset=utf8mb4"
)

engine = create_engine(CONN_STR)
# echo=True 会把每条 SQL 打印出来，调试时很有用，平时关掉
# engine = create_engine(CONN_STR, echo=True)

print("✅ 引擎创建完成")
print(f"   连接地址：{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
print(f"   驱动：pymysql（纯 Python 实现，不需要装 MySQL 客户端库）")

# =============================================================================
# 第 2 步：pd.read_sql() — 从数据库读到 DataFrame
# =============================================================================
# 语法：pd.read_sql(sql语句, engine)
# sql 可以是一个字符串，也可以是 SQLAlchemy 的 text() 对象。
# 查询结果直接变成 DataFrame，列名来自 SQL 的 SELECT 字段。

print("\n" + "=" * 72)
print("第 2 步：从数据库读取数据到 DataFrame")
print("=" * 72)

# --- 2.1 读取整张表 ---
df_emp = pd.read_sql("SELECT * FROM employees LIMIT 5", engine)
print("\n📋 employees 表（前 5 行）：")
print(df_emp)

# --- 2.2 多表 JOIN 查询 ---
# 查每个员工的当前薪资 + 部门
join_sql = """
    SELECT
        e.emp_no,
        CONCAT(e.first_name, ' ', e.last_name) AS full_name,
        d.dept_name,
        s.salary,
        t.title
    FROM employees e
    JOIN dept_emp de ON e.emp_no = de.emp_no
    JOIN departments d ON de.dept_no = d.dept_no
    JOIN salaries s ON e.emp_no = s.emp_no
    JOIN titles t ON e.emp_no = t.emp_no
    WHERE de.to_date = '9999-01-01'    -- 当前部门
      AND s.to_date = '9999-01-01'     -- 当前薪资
      AND t.to_date = '9999-01-01'     -- 当前职位
    LIMIT 8
"""
df_join = pd.read_sql(join_sql, engine)
print("\n📋 多表 JOIN 查询结果（前 8 行）：")
print(df_join)

# --- 2.3 聚合查询 ---
agg_sql = """
    SELECT
        d.dept_name AS 部门,
        COUNT(DISTINCT e.emp_no) AS 人数,
        ROUND(AVG(s.salary), 0) AS 平均薪资,
        MAX(s.salary) AS 最高薪资,
        MIN(s.salary) AS 最低薪资
    FROM employees e
    JOIN dept_emp de ON e.emp_no = de.emp_no
    JOIN departments d ON de.dept_no = d.dept_no
    JOIN salaries s ON e.emp_no = s.emp_no
    WHERE s.to_date = '9999-01-01'
    GROUP BY d.dept_name
    ORDER BY 平均薪资 DESC
"""
df_dept_stats = pd.read_sql(agg_sql, engine)
print("\n📋 部门薪资统计：")
print(df_dept_stats)

# =============================================================================
# 第 3 步：Python 侧清洗 + 加工
# =============================================================================
# 数据进了 DataFrame 之后，就跟 Day 1-4 学的 pandas 操作完全一样。
# SQL 负责取数，pandas 负责复杂逻辑——各做各擅长的事。

print("\n" + "=" * 72)
print("第 3 步：Python 侧数据加工")
print("=" * 72)

# 拉取所有员工完整数据
all_emp_sql = """
    SELECT
        e.emp_no,
        e.first_name,
        e.last_name,
        e.gender,
        e.birth_date,
        e.hire_date,
        d.dept_name,
        s.salary,
        t.title
    FROM employees e
    JOIN dept_emp de ON e.emp_no = de.emp_no
    JOIN departments d ON de.dept_no = d.dept_no
    JOIN salaries s ON e.emp_no = s.emp_no
    JOIN titles t ON e.emp_no = t.emp_no
    WHERE s.to_date = '9999-01-01'
      AND t.to_date = '9999-01-01'
"""
df_all = pd.read_sql(all_emp_sql, engine)
print(f"\n📋 拉取完整数据：{len(df_all)} 行 × {len(df_all.columns)} 列")

# 用 pandas 做一个 SQL 不太好写的操作：给每个部门的人按薪资排名
df_all["dept_salary_rank"] = df_all.groupby("dept_name")["salary"].rank(
    ascending=False, method="dense"
)
print("\n📋 各部门薪资排名（Top 3）：")
top3 = df_all[df_all["dept_salary_rank"] <= 3].sort_values(["dept_name", "dept_salary_rank"])
print(top3[["emp_no", "first_name", "last_name", "dept_name", "salary", "dept_salary_rank"]].to_string())

# 再看看薪资水平分级
def salary_level(s):
    if s < 20000:
        return "初级"
    elif s < 30000:
        return "中级"
    elif s < 40000:
        return "高级"
    else:
        return "资深"

df_all["salary_level"] = df_all["salary"].apply(salary_level)
print(f"\n📋 薪资等级分布：\n{df_all['salary_level'].value_counts()}")

# =============================================================================
# 第 4 步：df.to_sql() — 把 DataFrame 写回 MySQL
# =============================================================================
# 语法：df.to_sql(表名, engine, if_exists='...', index=False)
#
# if_exists 参数（重要！）：
#   'fail'    — 表已存在就报错（默认，最安全）
#   'replace' — 删掉旧表，建新表（开发时常用）
#   'append'  — 追加到旧表后面（生产常用）
#
# index=False：不要把 DataFrame 的行索引写进数据库（99% 的情况都要加）

print("\n" + "=" * 72)
print("第 4 步：DataFrame 写回 MySQL")
print("=" * 72)

# --- 4.1 部门统计表写回 ---
df_dept_stats.columns = ["department", "emp_count", "avg_salary", "max_salary", "min_salary"]
table_name = "dept_salary_stats"

df_dept_stats.to_sql(
    name=table_name,
    con=engine,
    if_exists="replace",   # 开发阶段：每次运行都重建
    index=False,
)
print(f"✅ 部门统计写入 `{table_name}` 表（{len(df_dept_stats)} 行）")

# 验证：读回来看看
verify = pd.read_sql(f"SELECT * FROM {table_name}", engine)
print(f"   验证读取：{len(verify)} 行")
print(verify)

# --- 4.2 员工薪资等级表写回 ---
table_name2 = "employee_salary_levels"

# 只导出需要的列，换个干净的列名
df_export = df_all[["emp_no", "first_name", "last_name", "dept_name", "salary", "salary_level"]].copy()
df_export.columns = ["emp_no", "first_name", "last_name", "department", "salary", "level"]

df_export.to_sql(
    name=table_name2,
    con=engine,
    if_exists="replace",
    index=False,
)
print(f"\n✅ 薪资等级写入 `{table_name2}` 表（{len(df_export)} 行）")

# =============================================================================
# 第 5 步：用 text() 执行任意 SQL（DDL / DML）
# =============================================================================
# pd.read_sql() 适合 SELECT 查询。
# 对于 CREATE TABLE / INSERT / UPDATE / DELETE，需要用 engine.connect() + text()。

print("\n" + "=" * 72)
print("第 5 步：执行非查询 SQL（DDL / DML）")
print("=" * 72)

with engine.connect() as conn:
    # --- 5.1 给表加个注释列 ---
    # MySQL 不支持 ADD COLUMN IF NOT EXISTS，用 try/except 兜底
    try:
        conn.execute(text("""
            ALTER TABLE employee_salary_levels
            ADD COLUMN note VARCHAR(200) DEFAULT NULL
        """))
        print("✅ 添加 note 列")
    except Exception:
        print("⏭️  note 列已存在，跳过 ALTER TABLE")

    # --- 5.2 批量 UPDATE：给资深员工加备注 ---
    conn.execute(text("""
        UPDATE employee_salary_levels
        SET note = '高价值人才，建议保留'
        WHERE level = '资深'
    """))
    print("✅ 更新资深员工备注")

    # --- 5.3 删除测试数据（演示 DELETE）---
    # conn.execute(text("DELETE FROM employee_salary_levels WHERE level = '初级'"))
    # print("✅ 删除初级员工记录")

    conn.commit()  # ★ 写操作必须 commit！否则连接关闭后回滚

# 验证 UPDATE 结果
print("\n📋 验证 UPDATE：")
result = pd.read_sql("""
    SELECT emp_no, first_name, last_name, level, note
    FROM employee_salary_levels
    WHERE note IS NOT NULL
""", engine)
print(result)

# =============================================================================
# 第 6 步：参数化查询 — 安全传参
# =============================================================================
# ★ 永远不要用 f-string 拼用户输入到 SQL！SQL 注入风险。
# 正确做法：用 text() 的参数绑定。

print("\n" + "=" * 72)
print("第 6 步：参数化查询")
print("=" * 72)

# 模拟：用户输入了一个部门名和最低薪资
target_dept = "Engineering"
min_salary = 60000

safe_sql = text("""
    SELECT emp_no, first_name, last_name, salary, department
    FROM employee_salary_levels
    WHERE department = :dept       -- :dept 是占位符
      AND salary > :min_sal        -- :min_sal 也是占位符
    ORDER BY salary DESC
""")

with engine.connect() as conn:
    result = conn.execute(
        safe_sql,
        {"dept": target_dept, "min_sal": min_salary}   # 字典传参
    )
    df_param = pd.DataFrame(result.fetchall(), columns=result.keys())

print(f"\n📋 参数化查询：部门={target_dept}，薪资 > {min_salary}")
print(df_param)

# pd.read_sql() 也支持参数：
df_param2 = pd.read_sql(
    safe_sql,
    engine,
    params={"dept": "Sales", "min_sal": 50000}
)
print(f"\n📋 同样的参数化查询（Sales 部门，薪资 > 50000）：")
print(df_param2)

# =============================================================================
# 第 7 步：完整流水线演示
# =============================================================================
# 模拟真实场景：从数据库取数 → pandas 清洗加工 → 写回新表

print("\n" + "=" * 72)
print("第 7 步：完整流水线 — 读 → 算 → 写")
print("=" * 72)

# Step A：读
pipeline_sql = """
    SELECT
        e.emp_no,
        e.first_name,
        e.last_name,
        d.dept_name AS department,
        s.salary,
        e.hire_date,
        YEAR(CURDATE()) - YEAR(e.hire_date) AS years_worked
    FROM employees e
    JOIN dept_emp de ON e.emp_no = de.emp_no
    JOIN departments d ON de.dept_no = d.dept_no
    JOIN salaries s ON e.emp_no = s.emp_no
    WHERE s.to_date = '9999-01-01'
"""
df_pipeline = pd.read_sql(pipeline_sql, engine)

# Step B：算 — 用 pandas 做数据处理
# 计算每个员工的薪资在部门内的百分位
df_pipeline["salary_pct"] = (
    df_pipeline.groupby("department")["salary"]
    .rank(pct=True) * 100      # pct=True 返回 0~1 的百分位
).round(1)

# 加各种标识列
df_pipeline["is_high_salary"] = df_pipeline["salary_pct"] >= 75
df_pipeline["is_senior"] = df_pipeline["years_worked"] >= 5
df_pipeline["talent_tag"] = df_pipeline.apply(
    lambda row: (
        "🌟 核心人才" if row["is_high_salary"] and row["is_senior"]
        else "📈 高潜新人" if row["is_high_salary"] and not row["is_senior"]
        else "🛡️ 稳定骨干" if not row["is_high_salary"] and row["is_senior"]
        else "🌱 成长中"
    ),
    axis=1,
)

# Step C：写 — 写回数据库
df_pipeline.to_sql(
    "employee_talent_analysis",
    engine,
    if_exists="replace",
    index=False,
)

print("✅ 流水线完成：")
print(f"   读取 {len(df_pipeline)} 行")
print(f"   写入 employee_talent_analysis 表")
print(f"\n📋 人才标签分布：")
print(df_pipeline["talent_tag"].value_counts())
print(f"\n📋 前 10 行结果：")
print(df_pipeline.head(10).to_string())

# =============================================================================
# 第 8 步：查看数据库里现在有哪些表
# =============================================================================
print("\n" + "=" * 72)
print("第 8 步：确认所有产出表")
print("=" * 72)

tables = pd.read_sql("SHOW TABLES", engine)
print(f"\n📋 employees 数据库当前所有表（{len(tables)} 张）：")
for i, row in tables.iterrows():
    table_name = row.iloc[0]
    count = pd.read_sql(f"SELECT COUNT(*) AS cnt FROM `{table_name}`", engine)
    print(f"   {i+1}. {table_name} — {count.iloc[0, 0]} 行")

# =============================================================================
# 核心概念总结
# =============================================================================
print("\n" + "=" * 72)
print("📝 核心概念总结")
print("=" * 72)

print("""
┌─────────────────────┬──────────────────────────────────────────────┐
│ 概念                 │ 一句话                                       │
├─────────────────────┼──────────────────────────────────────────────┤
│ create_engine()      │ 建连接工厂，惰性连接，真正用时才连           │
│ pd.read_sql()        │ SQL 查数据库 → DataFrame（只读 SELECT）       │
│ df.to_sql()          │ DataFrame → MySQL 表（自动建表+插数据）      │
│ text()               │ 包装 SQL 字符串，支持参数绑定（防注入）      │
│ if_exists='replace'  │ 开发用，每次覆盖重建；生产用 'append'        │
│ index=False          │ 不把 pandas 行索引写进数据库（99% 情况加）   │
│ conn.commit()        │ 写操作必须提交，否则连接关闭后丢失           │
│ 参数绑定 :name       │ 用 :变量名 占位，字典传值，防 SQL 注入       │
│ pymysql vs mysqlclient│ pymysql 纯 Python（无需装库）, mysqlclient C 扩展（更快）│
└─────────────────────┴──────────────────────────────────────────────┘

工作流：
  数据库 ──pd.read_sql()──▶ DataFrame ──pandas清洗──▶ DataFrame
                                                        │
                                                    df.to_sql()
                                                        │
                                                        ▼
                                                     数据库 ✨

面试可能问到：
  Q: pd.read_sql() 和 pd.read_csv() 区别？
  A: 数据来源不同，但产出的都是 DataFrame，后续操作完全一样。
     数据在哪不重要——pandas 统一了接口。

  Q: 为什么用 pymysql 而不是 mysql-connector？
  A: pymysql 纯 Python，pip install 就能用，无需系统库。
     SQLAlchemy 官方推荐配合 pymysql 或 mysqlclient。
""")

# =============================================================================
# 清理（可选）
# =============================================================================
# 如果想把演示产生的表删掉，取消下面的注释：
#
# with engine.connect() as conn:
#     conn.execute(text("DROP TABLE IF EXISTS dept_salary_stats"))
#     conn.execute(text("DROP TABLE IF EXISTS employee_salary_levels"))
#     conn.execute(text("DROP TABLE IF EXISTS employee_talent_analysis"))
#     conn.commit()
# print("🗑️  已清理演示表")

print("\n✅ Day 5-6 练习完成。")
print("   产出表：dept_salary_stats, employee_salary_levels, employee_talent_analysis")

# =============================================================================
# 🏋️ 自主练习：读 → 算 → 写
# =============================================================================
# 任务：
#   1. 从 salaries 表查出所有薪资记录
#   2. 用 pandas 找每个员工的历史最高/最低薪资
#   3. 算薪资涨幅（最高 - 最低）
#   4. 写回新表 emp_salary_range
#   5. 打印涨幅最大的 3 个人
#
# 提示：
#   - pd.read_sql("SELECT emp_no, salary FROM salaries", engine)
#   - df.groupby("emp_no")["salary"].agg(["max", "min"])
#   - df["increase"] = df["max"] - df["min"]
#   - df.to_sql("emp_salary_range", engine, if_exists="replace", index=False)
#   - df.nlargest(3, "increase")
#
# 预计 10 行以内。写出来后这课就真正掌握了。

print("\n" + "=" * 72)
print("🏋️ 自主练习：薪资涨幅分析")
print("=" * 72)

# ↓↓↓ 你的代码写在这里 ↓↓↓
all_salaries = pd.read_sql("SELECT emp_no, salary FROM salaries", engine)
salary_range = all_salaries.groupby("emp_no")["salary"].agg(["max", "min"])
salary_range["increase"] = salary_range["max"] - salary_range["min"]
salary_range.to_sql(
    "emp_salary_range",
    engine,
    if_exists="replace",
    index=True,   # 这里保留 index，因为 emp_no 是索引，写入表有意义
)
print("\n📋 涨幅最大的 3 个人：")
print(salary_range.nlargest(3, "increase"))


# ↑↑↑ 你的代码写在这里 ↑↑↑

print("\n📝 练习文件：phase1_day5_sqlalchemy_mysql.py")

