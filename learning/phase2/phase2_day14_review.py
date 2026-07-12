#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2 Day 14: Phase 2 总复习 + 代码整理
==========================================

学习目标：
  1. 把 Phase 2 全部知识点串成一张脑图
  2. 检查哪些概念还不扎实，回去补
  3. 确认代码/GitHub 干净整洁
  4. 整理面试卖点 —— 每个知识点一句话说清楚

不做新东西了。今天的产出是：
  - 一张 Phase 2 知识地图
  - 一份自检清单
  - 确认 GitHub 工程干净
"""

# ================================================================
# 第 1 关：Phase 2 知识点全览
# ================================================================
print("===== 第 1 关：画 Phase 2 知识地图 =====")

print("""
Phase 2 学了四周，分成四大块：

  ┌─────────────────────────────────────────────────────┐
  │  PySpark（Day 1-5）                                  │
  │  · SparkSession / DataFrame API                      │
  │  · select / filter / groupBy / agg / withColumn     │
  │  · 延迟计算（Lazy Evaluation）                      │
  │  · 分区（Partition）vs 分桶                         │
  │  · 窄依赖 vs 宽依赖                                  │
  │  · Shuffle 原理 + 优化                               │
  │  · 数据倾斜诊断 + 加盐法（Salt）                     │
  │  · JDBC 写 MySQL                                    │
  │  · F.countDistinct / dropDuplicates / split         │
  ├─────────────────────────────────────────────────────┤
  │  Airflow（Day 6-9）                                  │
  │  · DAG / Operator / Task 依赖（>>）                  │
  │  · schedule / start_date / catchup                  │
  │  · PythonOperator vs BashOperator                   │
  │  · retries / retry_delay / on_failure_callback      │
  │  · Sensor（FileSensor / ExternalTaskSensor）        │
  │  · Hook（DbApiHook）                                 │
  │  · context 字典 / **context 传参                    │
  │  · Airflow 3.x 模板变量变更（ds/logical_date）       │
  ├─────────────────────────────────────────────────────┤
  │  完整管道（Day 10-11 + 综合大练习）                  │
  │  · CSV → PySpark → MySQL → Airflow → 验证            │
  │  · argparse CLI（--step / --date）                  │
  │  · Config 类 + logger 双通道                         │
  │  · PipelineError 自定义异常                          │
  │  · 加盐法两阶段聚合（salt → agg1 → unsalt → agg2）  │
  │  · 加权平均（sum/sum，不是 avg of avg）             │
  │  · validate 自动化                                   │
  ├─────────────────────────────────────────────────────┤
  │  数据质量 + 版本控制（Day 12-13）                    │
  │  · Great Expectations 声明式验证                     │
  │  · DVC 数据版本管理                                  │
  │  · dvc.yaml 管道即代码                               │
  │  · 数据可追溯性                                      │
  └─────────────────────────────────────────────────────┘

TODO 1：拿一张白纸，不看上面的内容，自己默写这张图
对比看看漏了什么 —— 漏掉的就是你还不够熟的
""")


# ================================================================
# 第 2 关：核心概念自检
# ================================================================
print("\n===== 第 2 关：概念自检 =====")

print("""
对每个概念，自问三个问题：
  Q1: 这是什么？（一句话定义）
  Q2: 什么时候用？（场景）
  Q3: 哪里容易踩坑？（你实际遇到过的 bug）

不会的回答就去翻代码/笔记，不要跳过。
""")

checklist = [
    # PySpark
    ("窄依赖 vs 宽依赖", "你的 refund_etl.py 里，哪一步是窄依赖？哪一步是宽依赖？"),
    ("Shuffle", "groupBy().agg() 为什么触发 Shuffle？Shuffle 的代价是什么？"),
    ("数据倾斜", "你项目中 store_1 占 40% 是倾斜吗？怎么发现的？怎么解决的？"),
    ("加盐法", "加盐为什么能解决倾斜？两步聚合每一步在算什么？"),
    ("延迟计算", "Spark 为什么等到 action 才执行？有哪些常用 action？"),

    # Airflow
    ("DAG", "你项目里 DAG 叫什么名字？schedule 是什么？catchup 为什么设 False？"),
    ("PythonOperator", "和 BashOperator 比，选 PythonOperator 的理由？"),
    ("retry vs retry_delay", "失败后第 1 次重试隔多久？最多重试几次？"),
    ("**context vs context", "为什么 run_step(step, **context) 而不是 run_step(step, context)？"),
    ("Sensor", "哪些场景需要 Sensor？poke vs reschedule 模式区别？"),

    # 管道设计
    ("完整管道 6 步", "load → clean → salt → aggregate → save → validate 每步做什么？"),
    ("加权平均", "avg_refund_amount 为什么不能用 avg of avg？什么情况下用 sum/sum？"),
    ("Config 类", "为什么把配置抽到 Config 类而不是硬编码？改了 MySQL 密码要改几个文件？"),
    ("PipelineError", "自定义异常的作用？跟直接 raise Exception 比好在哪？"),
    ("日志双通道", "logger 同时输出到文件和控制台的好处？各自的用途？"),

    # 数据质量
    ("声明式 vs 命令式", "GE 的 expect_column_min_to_be_between 和手写 if 本质区别？"),
    ("DVC staging", "dvc.yaml 里的 stage, deps, outs 各代表什么？"),
    ("数据可追溯", "某一天的数据跑错了，如何找到是哪个脚本版本造成的？"),
]

for i, (concept, question) in enumerate(checklist, 1):
    print(f"\n  {i:02d}. {concept}")
    print(f"      Q2: {question}")


# ================================================================
# 第 3 关：面试卖点整理
# ================================================================
print("\n===== 第 3 关：30 秒卖点 =====")

print("""
面试官问"你做过什么数据工程相关的东西"，你的回答框架：

  开场（1 句）：
  "我独立搭建了一套电商数据的 ETL 管道，用 PySpark 做分布式处理，
   Airflow 做调度，MySQL 做存储，并且集成了数据质量验证。"

  展开（3 个亮点，任选其一展开）：
  亮点 1 —— 处理数据倾斜：
    "数据有严重倾斜，单 key 占 40% 的量。我用加盐法做了两步聚合，
     把倾斜 key 打散到多个分区，聚合时间降了 [X]%。"

  亮点 2 —— 完整性：
    "不是一个脚本，是完整的生产线。从数据生成、清洗、聚合、入库、
     Airflow 调度、到自动验证，全链路打通。代码在 GitHub 上可以直接看。"

  亮点 3 —— 工程规范：
    "做了 Config 隔离、自定义异常、双通道日志、命令行参数设计，
     不只是'能跑'，是按生产标准写的。"

  收尾（1 句）：
    "同时这个项目全程自学完成，证明我有独立上手新技术栈的能力。"
""")

# TODO 3：用你自己的话说一遍上面的 30 秒卖点
# 录音 + 计时，控制在 30 秒以内，练 5 遍
# 为什么录音？—— 嘴巴跟不上的地方，就是你脑子里还不熟的地方


# ================================================================
# 第 4 关：GitHub 工程检查
# ================================================================
print("\n===== 第 4 关：GitHub 验收 =====")

print("""
面试官打开你的 GitHub，第一眼看到什么？

  仓库名: data-engineering
  README 应该说明:
    ① 这是什么项目
    ② 用到了什么技术栈
    ③ 怎么跑起来（pip install ... && python main.py）
    ④ 架构图（文字 ASCII 画也行）

  目录结构应该干净:
    data-engineering/
    ├── README.md           ← 必须有！
    ├── pipeline_etl.py      ← 第一个管道
    ├── refund_etl.py        ← 退款分析管道
    ├── refund_config.py     ← 配置
    ├── airflow/dags/        ← DAG
    ├── data/                ← 数据生成脚本
    └── .gitignore           ← 不该推送的一律不推送

确认：
  □ 没有虚拟环境文件（bin/lib/include）
  □ 没有 CSV 数据文件
  □ 没有密码/Token
  □ 没有 .pyc / __pycache__
  □ README.md 已写
""")

# TODO 4-1：写 README.md
# 参考结构：
#
# # 数据工程学习项目
#
# ## 技术栈
# - PySpark, Apache Airflow, MySQL, pandas, Great Expectations, DVC
#
# ## 项目
# ### 电商退款分析 ETL 管道
# - 完整链路：数据生成 → 清洗 → 聚合 → 入库 → 调度 → 验证
# - 含数据倾斜处理（加盐法）
# - Airflow DAG 每日自动执行
#
# ## 快速开始
# ```bash
# pip install -r requirements.txt
# python refund_etl.py --date 2025-06-01
# ```
#
# ## 目录结构
# ...

# TODO 4-2：写 requirements.txt
# pip freeze > requirements.txt
# 但要删掉不需要的包，只保留项目实际用到的

# TODO 4-3：最后一次检查 git status
# 确认没有不该提交的文件


# ================================================================
# 第 5 关：Phase 2 和 Phase 1 怎么串起来
# ================================================================
print("\n===== 第 5 关：全景图 =====")

print("""
回顾你全部学习路径：

  Phase 1（地基）：
    pandas → numpy → SQLAlchemy → ETL 脚本 → 重构 → 命令行工具

    产出: 知道怎么写"能用的"数据处理脚本

  Phase 2（上梁）：
    PySpark → Airflow → 完整管道 → 数据倾斜 → 加盐法
    → 综合大练习 → GE 质量验证 → DVC 版本控制

    产出: 能搭"生产级的"数据管道

  Phase 3（装修，即将开始）：
    向量数据库 → RAG 管道 → AI 数据质量 → 面试刷题 → 投递

    产出: 数据 + AI 的交叉能力，面试即战力

  整个路径的主线: 从单机 → 分布式 → AI 嵌入
  不是三个独立的东西，是一个东西一层一层往上加
""")

# TODO 5：画一张自己的全景学习地图
# 纸和笔，把 Phase 0（SQL）→ Phase 1 → Phase 2 的知识点
# 画成一张 roadmap，标注哪些是你觉得"最扎实"的，哪些是"还需要练"的


# ================================================================
# 自检：Phase 2 全部完成
# ================================================================
print("\n===== Phase 2 完成 =====")
print("""
今天把所有 TODO 过完，确认：

  □ 概念自检清单（第 2 关）全部能口头回答
  □ 30 秒卖点（第 3 关）能流畅讲出
  □ GitHub README 已写
  □ requirements.txt 生成好了
  □ Git 干净，全部 push
  □ 知识地图画出来了

然后 commit 今天的学习:

  git add .
  git commit -m "Phase 2 complete: GE quality checks + DVC version control + review"
  git push

下一站 → Phase 3：向量数据库 + RAG 管道 🚀
""")
