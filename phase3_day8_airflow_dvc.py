"""
============================================================
Phase 3 · Day 8  用 Airflow 调度整条数据管道 + DVC 管数据版本
============================================================
先想清楚一件事（面试常被追问）：
  Airflow 调度的【不是】"语义检索"那一步。检索是用户来一次跑一次的
  实时服务（以后包成 FastAPI），毫秒级，跟 Airflow 无关。
  Airflow 管的是它【上游】那条"生产向量库"的离线批处理管道：

     采集 → 清洗+校验 → 向量化 → 产出 vectors.npy
     （慢、重、批量、要定期重跑、步骤有依赖、会失败要重试要告警）

  这正是数据工程师和"会用 pandas 的人"的分水岭：
  不只是"能写脚本"，而是"能把脚本编排成定时/按依赖/自动/可监控的生产管道"。

本节复用你 Phase 2 的 Airflow 技能（DAG/依赖/重试/告警），
只是把它用到 Phase 3 这条真实管道上。三个步骤脚本你都已经写好了，
这里只负责"把它们串成一个 DAG"。

【本节规则】每个 TODO 自己写，卡住看提示。Airflow 你学过，这里主要是回忆+套用。
============================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
# ⚠️ Airflow 3.x 的 BashOperator 在 providers.standard 里（你的版本是 3.2.2）
from airflow.providers.standard.operators.bash import BashOperator

# 项目路径 & venv 的 python（BashOperator 默认不在项目目录、也不认 venv，要写全）
VENV = "/Users/thatguy/de-venv"
PY = f"{VENV}/bin/python"


# ============================================================
# 告警回调：任意任务失败时自动被调用
# ------------------------------------------------------------
def alert_on_failure(context):
    # TODO(2): 打印一条告警日志，说明"哪个任务失败了"
    #   失败的任务名： context["task_instance"].task_id
    #   （真实项目这里会发邮件/钉钉/飞书；我们 print 到日志就行，能被 Airflow 记录）
    task_id = context["task_instance"].task_id
    print(f"任务失败告警: {task_id} 失败!")


# ============================================================
# TODO(1): default_args —— 应用到所有任务的默认配置
# ------------------------------------------------------------
# 要求：失败重试 2 次，每次间隔 5 分钟；并挂上上面的告警回调
#   retries      = 2
#   retry_delay  = timedelta(minutes=5)
#   on_failure_callback = alert_on_failure
# ============================================================
default_args = {
    "owner": "thatguy",
    "retries": 2,
    "retry_delay": timedelta(minutes = 5),
    "on_failure_callback": alert_on_failure,
}


with DAG(
    dag_id="phase3_corpus_pipeline",
    default_args=default_args,
    description="日韩语料语义检索——离线数据管道（采集→清洗→向量化）",
    start_date=datetime(2026, 7, 1),
    schedule="0 3 * * 1",   # cron：每周一凌晨 3 点。（回忆：分 时 日 月 周）
    catchup=False,          # 不补跑历史（否则会把 7/1 到今天每周都补跑一遍）
    tags=["phase3", "rag"],
) as dag:

    # —— 步骤1 采集（已给你做示范，看清 bash_command 的套路）——
    # 关键：先 cd 到项目目录（脚本里用的是相对路径 data/...），再用 venv 的 python
    ingest = BashOperator(
        task_id="ingest",
        bash_command=f"cd {VENV} && {PY} phase3_day3_tatoeba_ingest.py",
    )

    # —— 步骤2 清洗+校验 ——
    # TODO(3a): 仿照 ingest 写 bash_command，脚本是 phase3_day4_5_clean_validate.py
    #   ⚠️ 但 PySpark 需要两个环境变量（你 Day 4-5 踩过的坑），命令里要先 export：
    #        export PYSPARK_PYTHON={PY} && export PYSPARK_DRIVER_PYTHON={PY} &&
    #   完整形如：
    #        cd {VENV} && export PYSPARK_PYTHON={PY} && export PYSPARK_DRIVER_PYTHON={PY} && {PY} phase3_day4_5_clean_validate.py
    clean = BashOperator(
        task_id="clean_validate",
        bash_command=f"cd {VENV} && export PYSPARK_PYTHON={PY} && export PYSPARK_DRIVER_PYTHON={PY} && {PY} phase3_day4_5_clean_validate.py",
    )

    # —— 步骤3 向量化 ——
    # TODO(3b): 仿照 ingest，脚本是 phase3_vectorize.py（已有幂等保护，向量在就跳过）
    vectorize = BashOperator(
        task_id="vectorize",
        bash_command=f"cd {VENV} && {PY} phase3_vectorize.py",
    )

    # ============================================================
    # TODO(4): 设任务依赖 —— 采集 → 清洗 → 向量化（用 >>）
    #   一行：  ingest >> clean >> vectorize
    # ============================================================
    ingest >> clean >> vectorize


# ============================================================
# 部署 & 运行（填完 TODO 后，在终端做）
# ------------------------------------------------------------
#  1) 建 dags 目录并把本文件放进去（Airflow 只认 ~/airflow/dags 里的 DAG）：
#       mkdir -p ~/airflow/dags
#       cp ~/de-venv/phase3_day8_airflow_dvc.py ~/airflow/dags/
#     （注意：TODO 没填完时文件里有 ___，Airflow 会解析报错，属正常，填完再 cp）
#
#  2) 起 Airflow：
#       cd ~/de-venv && source bin/activate && airflow standalone
#     浏览器开 http://localhost:8080 （用户名/密码看终端打印的 simple auth）
#
#  3) 在 UI 找到 DAG "phase3_corpus_pipeline" → 打开 → 右上角 Trigger 手动跑一次
#     看三个任务依次变绿(ingest→clean_validate→vectorize)；点任务可看日志
#     vectorize 因为向量已存在会秒过（打印"跳过"）——这就是幂等的好处
#
#  4) 【重要】用完 Ctrl+C 关掉 airflow standalone，别又让它跑好几天烤机器
#
# ============================================================
# DVC：给数据 / 向量打版本（复用你 Day 13 的 DVC）
# ------------------------------------------------------------
#  为什么：vectors.npy 有 386MB，git 不该塞大二进制文件。
#          DVC 用一个几行的 .dvc "指针"进 git，真文件另存 —— 代码和数据都能回溯。
#
#  在终端（先确认 ~/de-venv 是 git 仓库：git status；不是就先 git init）：
#       cd ~/de-venv
#       dvc init                                   # 若还没初始化过
#       dvc add data/emb/vectors.npy               # 生成 vectors.npy.dvc + .gitignore
#       dvc add data/cleaned/tatoeba_clean.parquet
#       git add data/emb/vectors.npy.dvc data/emb/.gitignore data/cleaned/*.dvc .gitignore
#       git commit -m "phase3: DVC 版本化清洗语料与向量"
#  以后语料更新 → 重新 dvc add → git commit，就有了"数据版本历史"。
#
# 全部跑通后叫我批改 DAG，然后我们把这条写进简历。
# ============================================================
