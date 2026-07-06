#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2 Day 12: Great Expectations —— 数据质量自动化
=====================================================

学习目标：
  1. 理解数据质量框架是什么，为什么不能只靠手工检查
  2. 用 Great Expectations 给管道加自动化质量断言
  3. 产出自动化的数据质量报告

场景：退款 ETL 管道跑完之后，自动验证数据质量
  之前 validate_results() 是手写的几个 if 检查
  现在用 GE 框架来做 —— 验证规则可复用、可扩展、自动生成报告

核心理念：
  写的不是"检查代码"，而是"数据应该长什么样的声明"

  你写: "refund_rate 应该在 0 到 1 之间"
  GE 做: 自动跑这条规则 + 记录哪行没过 + 生成 HTML 报告 + 记入历史
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

声明式验证（GE 的方式）：
  validator.expect_column_min_to_be_between("refund_amount", 0, None)
  → 你声明"这列的最小值应该在 0 以上"，框架自己跑检查

为什么重要？
  - 20 个 if 的 validate 函数 → 维护成本越来越高
  - 每次改规则要改代码，容易出错
  - 面试官问"你的数据质量怎么保证" → 你说"我用 Great Expectations"
    比说"我写了几个 if"强一个级别
""")

# TODO 1：用你自己的话写一下 —— 声明式验证比命令式好在哪？
# （至少两个理由）


# ================================================================
# 第 2 关：安装并初始化 Great Expectations
# ================================================================
print("\n===== 第 2 关：环境准备 =====")

print("""
在终端执行：

  pip install great_expectations

然后初始化 GE 项目：

  cd ~/de-venv
  great_expectations init

这会创建 gx/ 目录，里面包含：
  gx/
  ├── great_expectations.yml   ← 主配置文件
  ├── expectations/             ← 你写的验证规则（json）
  ├── checkpoints/              ← 验证跑点（可以定时执行）
  ├── plugins/                  ← 自定义插件
  └── uncommitted/
      ├── config_variables.yml  ← 敏感配置（密码等）
      └── data_docs/            ← 生成的 HTML 报告
""")

# TODO 2：安装 GE 并初始化。把 great_expectations.yml 里关键配置
# （datasource、store 路径）看一遍，用自己的话注释每块是干什么的


# ================================================================
# 第 3 关：连接数据源
# ================================================================
print("\n===== 第 3 关：连接 MySQL 数据源 =====")

print("""
GE 需要知道"我要验证什么数据"。数据源有三种连接方式：

  Filesystem:   直接读 CSV / Parquet 文件
  SQL:          连数据库，验证表的内容
  Spark:        通过 PySpark DataFrame 验证

我们要验证 MySQL 里的 daily_refund_report 表，选择 SQL 方式。

连接配置在 great_expectations.yml 里加一个 datasource：
  类型: SQL
  连接串: mysql+pymysql://root:@localhost:3306/refund_db
  要验证的表: daily_refund_report
""")

# TODO 3-1：在 great_expectations.yml 里配置 MySQL 数据源
# 提示：需要先 pip install pymysql
# 提示：参考 GE 官方文档 "How to connect to a SQL database"

# TODO 3-2：跑通连接测试
# 在终端执行：great_expectations datasource list
# 确认你的 MySQL 数据源出现在列表里


# ================================================================
# 第 4 关：写第一组验证规则
# ================================================================
print("\n===== 第 4 关：日常数据质量检查 =====")

print("""
对 daily_refund_report 这张表，每天跑完管道后至少要验证：

  常规检查：
    1. total_orders > 0                ← 有数据写入
    2. total_refund >= 0              ← 退款金额非负
    3. refund_rate BETWEEN 0 AND 1    ← 退款率合理范围
    4. product_category 不能为 NULL   ← 品类完整
    5. report_date 等于今天日期       ← 日期正确

  业务检查：
    6. total_refund 在历史均值的 ±3 个标准差内 ← 异常检测
    7. distinct_users > 0             ← 有真实用户
    8. store_id 去重数量 ≥ 预期最小值  ← 数据覆盖足够

GE 里有三种方式写规则：

  A. 交互式（Jupyter Notebook）：
      在终端运行 great_expectations suite new
      → 选 datasource → 选表 → GE 自动分析数据 → 生成规则

  B. 手动写 JSON：
      在 expectations/ 目录下创建 .json 文件，格式如：
      {
        "expectation_type": "expect_column_min_to_be_between",
        "kwargs": {
          "column": "refund_rate",
          "min_value": 0,
          "max_value": 1
        }
      }

  C. Python API：
      validator.expect_column_min_to_be_between("refund_rate", 0, 1)
""")

# TODO 4-1：使用方式 A（交互式），创建第一套验证规则
# 运行 great_expectations suite new → 跟着向导走 → 保存为 refund_quality_suite

# TODO 4-2：手动补一条规则 —— 验证 total_refund 不超过历史最大值 50000
# 把这条规则加到 refund_quality_suite 的 JSON 文件里

# TODO 4-3：用 Python API 再写一套轻量的验证，就写在下面
# 目标：connect 到 datasource → 创建 batch（指定 report_date）→ 跑 5 条规则


# ================================================================
# 第 5 关：跑验证 + 看报告
# ================================================================
print("\n===== 第 5 关：执行验证 =====")

print("""
两种执行方式：

  CLI（适合 Airflow 调度）：
    great_expectations checkpoint run refund_checkpoint

  Python（适合嵌入 ETL 脚本）：
    context = gx.get_context()
    checkpoint_result = context.run_checkpoint(
        checkpoint_name="refund_checkpoint"
    )

跑完之后：
  - 自动生成 HTML 报告在 gx/uncommitted/data_docs/local_site/
  - 记录每次验证结果的历史（可以看趋势："上个月通过了 95 次，失败了 3 次"）
  - 失败的规则有详细的期望值 vs 实际值对比
""")

# TODO 5-1：跑一次 refund_checkpoint，截图生成的 HTML 报告
# 标注报告中每一部分在说什么

# TODO 5-2：故意往 MySQL 里插入一条脏数据（total_refund = -100）
# 再跑一次 checkpoint，看报告会不会标记失败——验证"验证"本身是有效的


# ================================================================
# 第 6 关：对接 Airflow
# ================================================================
print("\n===== 第 6 关：GE 集成到 Airflow DAG =====")

print("""
管道流程变成：

  新流程：
  ① PySpark ETL 完成
  ② GE Checkpoint 自动跑
  ③ 如果验证全过 → DAG 标记为 success
  ④ 如果验证不过 → 发告警 + 记录哪些规则没过
  ⑤ HTML 报告自动存档

  旧流程：
  ① PySpark ETL
  ② validate_results() ← 只有 4 个 if
  ③ 没了

用 PythonOperator 在 refund_pipeline.py 里加一个任务：

  def run_ge_check(**context):
      # 在子进程里跑 GE checkpoint
      cmd = [
          sys.executable, "-m", "great_expectations",
          "checkpoint", "run", "refund_checkpoint"
      ]
      result = subprocess.run(cmd, capture_output=True, text=True)
      if result.returncode != 0:
          raise ValueError(f"数据质量验证失败: {result.stderr}")
      return result.stdout

  ge_check = PythonOperator(
      task_id="quality_check",
      python_callable=run_ge_check,
  )

  run_etl >> ge_check

这样每次管道跑完，GE 自动验证，不通过就告警。
""")

# TODO 6：在 refund_pipeline.py 里加 GE 验证步骤
# 1. 创建 run_ge_check 函数
# 2. 创建 PythonOperator
# 3. 设 run_etl >> ge_check
# 4. 手动触发 DAG，确认 GE 步骤跑通


# ================================================================
# 学完自检
# ================================================================
print("\n===== 学完自检 =====")
questions = [
    "声明式验证和命令式验证的区别？你更喜欢哪个？为什么？",
    "GE 的 Suite / Checkpoint / Expectation 分别是什么？",
    "GE 和之前手写 validate_results() 相比，哪些场景下 GE 更合适？",
    "面试官问'你怎么保证数据质量'，用 3 句话回答",
]
for i, q in enumerate(questions, 1):
    print(f"  {i}. {q}")

print("\n🚀 下一步 → Day 13：DVC 数据版本控制")
