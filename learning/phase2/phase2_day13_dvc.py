#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2 Day 13: DVC —— 数据和模型版本控制
============================================

学习目标：
  1. 理解为什么数据也需要版本控制（Git 管代码，DVC 管数据）
  2. 用 DVC 管理你的 CSV / 模型 / 管道配置
  3. 把 DVC 和 Git 绑定，一次 commit 同时记录代码和数据版本

场景：你的 refund_etl.py 每次改逻辑，跑出来的结果不同
  你需要知道"这一版 MySQL 数据是跑哪个版本的 Python 代码生成的"
  这对数据工程来说不是 nice-to-have，是必须

核心理念：
  Git 管源代码 → git commit
  DVC 管数据文件 → dvc add / dvc push
  用 dvc.yaml 描述管道步骤 → dvc repro（一键重跑整条管道）
"""

# ================================================================
# 第 1 关：理解"数据也需要版本控制"
# ================================================================
print("===== 第 1 关：为什么 Git 不够 =====")

print("""
一个常见的数据工程事故：

  下午 3:00  你改了 refund_etl.py 里的退款率计算公式
  下午 3:05  运行管道，生成了新的 daily_refund_report 数据
  下午 5:00  业务方说"今天的数据跟昨天差好多，你改了啥？"

  你：我改了 refund_etl.py 第 75 行...
  业务方：那你昨天跑的是哪个版本？
  你：呃...

如果有 DVC：
  dvc log daily_refund_report.csv
  → 最新版本：commit a1b2c3，用的 refund_etl.py v3
  → 上一版本：commit d4e5f6，用的 refund_etl.py v2

  一条命令看到数据是怎么来的。

Git 为什么管不了数据？
  - Git 对二进制/大文件效率极低（CSV 100MB → git 直接炸）
  - Git 不记录"这个 CSV 是用哪个脚本 + 哪个参数生成的"
  - DVC 是专门为这个设计的
""")

# TODO 1：用自己的话写 —— 什么情况下你会需要回查"这个数据是用哪个脚本生成的"？
# 至少举一个你实际遇到过的例子（可以从退款项目里想）etl处理后的数据自己不太满意时，会需要回查


# ================================================================
# 第 2 关：安装 + 初始化 DVC
# ================================================================
print("\n===== 第 2 关：环境准备 =====")

print("""
在终端执行：

  pip install dvc

然后初始化（在项目目录下）：

  cd ~/de-venv
  dvc init

初始化会创建 .dvc/ 目录，并提示你：
  git commit -m "Initialize DVC"

是的，DVC 和 Git 是绑在一起的——DVC 的元数据用 Git 管理
""")

# TODO 2-1：安装 DVC + 初始化
# 看看 .dvc/ 目录下有什么文件，用自己的话写注释

# TODO 2-2：把 DVC 初始化的 .dvc/.gitignore 等文件提交到 Git
# git add .dvc .dvcignore && git commit -m "init dvc"


# ================================================================
# 第 3 关：用 DVC 管理数据文件
# ================================================================
print("\n===== 第 3 关：dvc add —— 追踪数据文件 =====")

print("""
假设你想追踪生成的数据 CSV：

  # 先把 refund_etl.py 生成的 CSV 拷贝一份到项目里
  cp data/refund_orders.csv data/refund_orders_v1.csv

  # 用 DVC 追踪这个文件
  dvc add data/refund_orders_v1.csv

会发生什么：
  1. DVC 把文件内容存到 .dvc/cache/ 里
  2. 生成 data/refund_orders_v1.csv.dvc —— 这是一个小文本文件，记录文件路径 + 内容 hash
  3. .gitignore 自动加入 refund_orders_v1.csv（Git 不再追踪原文件）

之后：
  git add data/refund_orders_v1.csv.dvc .gitignore
  git commit -m "Add refund data v1"

  # 如果文件变更了，重新 dvc add，dvc 会自动产生新版本

工作原理：
  Git 管 .dvc 文件（几 KB）
  DVC 管实际数据（几十 MB 到几 GB）
  .dvc 文件里的 hash 指向缓存中的真实数据
""")

# TODO 3-1：用 dvc add 追踪一个 CSV 文件（任何你有的都行）
# 观察生成的 .dvc 文件内容，写注释说明每一行的含义 
# md5  —— 文件内容的指纹 hash，内容变 hash 就变，DVC 靠这个识别版本
# size —— 文件大小（字节），用来快速检查文件是否损坏
# hash —— hash 算法类型（md5），确保校验方式一致
# path —— 原文件名（不含路径，跟 .dvc 文件同级）

# TODO 3-2：改一下那个 CSV（比如加一行），再 dvc add 一次
# 观察新的 .dvc 文件 hash 是否变了 变了
# 用 dvc checkout 切回上一个版本，看 CSV 是否恢复了


# ================================================================
# 第 4 关：dvc.yaml —— 定义管道步骤
# ================================================================
print("\n===== 第 4 关：管道即代码 =====")

print("""
这是 DVC 最核心的功能 —— 用 YAML 描述你的管道

在项目根目录创建 dvc.yaml：

  stages:
    load:
      cmd: python refund_etl.py --step extract --date 2025-06-01
      deps:
        - data/refund_orders.csv    ← 依赖：数据源
        - refund_etl.py             ← 依赖：脚本
      outs:
        - data/raw_loaded.parquet   ← 产出：中间数据

    transform:
      cmd: python refund_etl.py --step transform --date 2025-06-01
      deps:
        - data/raw_loaded.parquet   ← 上一步的产出是这一步的依赖
        - refund_etl.py
      outs:
        - data/aggregated.parquet

    load_to_mysql:
      cmd: python refund_etl.py --step load --date 2025-06-01
      deps:
        - data/aggregated.parquet
        - refund_config.py

为什么要这样写？
  1. dvc repro 一键重跑整条管道，只跑变更的部分
     → 改了 refund_etl.py → 自动重跑三步
     → 只改了 MySQL 连接串 → 只重跑最后一步

  2. dvc dag 可视化管道依赖图
     → 面试官一眼看懂你的管道设计

  3. 任何人 clone 你的项目，dvc repro 一键复现你的结果
     → 面试官可以验证你的代码是不是真能跑
""")

# TODO 4-1：在你的 de-venv/ 项目里创建 dvc.yaml
# 把退款 ETL 的三个步骤（extract / transform / load）定义为 stage
# 提示：每个 stage 需要 cmd / deps / outs 三个字段

# TODO 4-2：运行 dvc dag 看管道依赖图
# 截图保存

# TODO 4-3：改一下 refund_etl.py 某一行，然后 dvc repro
# 观察 DVC 是否只重跑了"需要重跑的部分"


# ================================================================
# 第 5 关：DVC 远程存储
# ================================================================
print("\n===== 第 5 关：数据存在哪里 =====")

print("""
DVC 本地缓存（.dvc/cache/）只在你自己的电脑上
如果要共享给团队或备份，需要配置远端存储：

  # Google Drive
  dvc remote add -d myremote gdrive://folder_id

  # AWS S3
  dvc remote add -d myremote s3://my-bucket/data

  # 本地目录（最简单）
  dvc remote add -d myremote /tmp/dvc-storage

  dvc push  ← 把本地缓存推到远端
  dvc pull  ← 从远端拉数据到本地

对于你的情况：
  用本地目录就够了（学习目的）
  面试时提一句"支持配置 S3/GCS 远程存储"就行
""")

# TODO 5：配置一个本地 remote，把数据 push 过去再 pull 回来
# dvc remote add -d localstorage /tmp/dvc-test
# dvc push
# 删掉本地 .dvc/cache/ 然后 dvc pull —— 确认数据能恢复


# ================================================================
# 第 6 关：实际工作流
# ================================================================
print("\n===== 第 6 关：日常使用流程 =====")

print("""
每天管道跑完后的标准操作：

  # 1. 管道跑完
  python refund_etl.py --date 2026-07-06

  # 2. 新数据入库 → DVC 追踪
  dvc add data/refund_orders.csv

  # 3. 如果有模型/配置文件变更，一起追踪
  dvc add refund_config.py

  # 4. Git 记录 DVC 元数据
  git add data/refund_orders.csv.dvc dvc.lock
  git commit -m "pipeline run 2026-07-06: data updated"

  # 5. DVC 数据推远端（如果有配置）
  dvc push

  # 6. Git 代码推远端
  git push

这样一次 commit 同时记录了：
  - 代码版本（Git）
  - 数据版本（DVC）
  - 依赖关系（dvc.yaml）
  - 管道运行参数（dvc.lock 锁定了这次运行用了哪些依赖版本）
""")

# TODO 6：把上面的工作流实际操作一遍
# 用 refund_etl.py 生成一次数据 → dvc add → git commit → 确认 dvc.lock 生成


# ================================================================
# 第 7 关：面试话术
# ================================================================
print("\n===== 第 7 关：面试怎么讲 DVC =====")

print("""
面试官问："你们的代码和数据怎么管理版本？"

标准回答框架（30秒）：
  1. 代码用 Git 管理，数据用 DVC 管
  2. DVC 把大文件存远程，本地只留一个 .dvc 文件（几 KB）指向实际数据
  3. dvc.yaml 描述管道每一步的依赖关系，dvc repro 一键复现
  4. Git commit 同时记录代码和数据版本 —— 任何一次跑批的结果都可追溯

关键词：可追溯、可复现、管道即代码、Data Lineage
""")

# TODO 7：用自己的话说一遍上面那个框架
# 录音或口头讲，不看稿，讲到流畅为止


# ================================================================
# 学完自检
# ================================================================
print("\n===== 学完自检 =====")
questions = [
    "DVC 和 Git 的关系是什么？DVC 存了什么，Git 存了什么？",
    "dvc.yaml 里的 stage 包含哪三个核心字段？deps 和 outs 分别是干嘛的？",
    "dvc repro 跑完后，改了 refund_etl.py，再跑一次 repro，哪些 stage 会重跑？为什么？",
    "面试官问'数据可追溯性'是什么意思，用你的退款项目举例说明",
    "GE 和 DVC 放在一起用，画一个完整的管道流程图",
]
for i, q in enumerate(questions, 1):
    print(f"  {i}. {q}")

print("\n🚀 下一步 → Day 14：Phase 2 总复习 + 代码整理")
