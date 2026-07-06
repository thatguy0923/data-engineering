#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 1 Day 7-8: 自动数据处理脚本（命令行 ETL 流水线）
======================================================

学习目标：
  1. 把 Day 3-4 的清洗逻辑 + Day 5-6 的入库逻辑，封装成可复用脚本
  2. argparse — 命令行参数（--input / --output / --db / --report）
  3. logging — 替代 print，分级别记录日志（DEBUG/INFO/WARNING/ERROR）
  4. 函数拆分 — read / clean / save / report 各司其职
  5. if __name__ == "__main__" — 既能直接跑，也能被 import

使用方式：
  # 基础用法：只输出 CSV
  python phase1_day7_etl_pipeline.py --input data_employees_dirty.csv --output data_clean.csv

  # 完整用法：输出 CSV + 入库 + 出报告
  python phase1_day7_etl_pipeline.py --input data_employees_dirty.csv --output data_clean.csv --db --report report.txt

  # 查看帮助
  python phase1_day7_etl_pipeline.py --help

为什么这很重要？
  实际工作中的数据处理脚本不是 jupyter notebook——
  而是可以在服务器上定时执行的命令行脚本。这一课就是从"探索代码"到"生产代码"的转变。
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# =============================================================================
# 第 1 步：配置日志 — 替代 print
# =============================================================================
# logging 比 print 好在哪？
#   ① 分级：DEBUG(调试) < INFO(正常流程) < WARNING(警告) < ERROR(错误)
#   ② 带时间戳和位置，排查问题快
#   ③ 可以直接输出到文件

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# =============================================================================
# 第 2 步：核心清洗函数（从 Day 3-4 提炼）
# =============================================================================

def clean_employee_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    对脏员工数据执行标准清洗流程。
    输入：脏 DataFrame
    输出：干净 DataFrame
    """
    logger.info(f"开始清洗，原始数据 {len(df)} 行 × {len(df.columns)} 列")

    # --- 2.1 去重 ---
    dup_before = df.duplicated().sum()
    df = df.drop_duplicates()
    logger.info(f"去重：移除 {dup_before} 行重复数据")

    # --- 2.2 缺失值处理 ---
    null_before = df.isnull().sum().sum()
    logger.debug(f"缺失值统计：\n{df.isnull().sum()}")

    df["name"] = df["name"].fillna("员工_" + df["emp_id"].astype(str))
    df["department"] = df["department"].fillna("待分配")
    df["manager"] = df.groupby("department")["manager"].transform(
        lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "未知")
    )

    # 薪资：组内中位数优先，全局中位数兜底
    df["salary"] = df.groupby("department")["salary"].transform(
        lambda x: x.fillna(x.median())
    )
    df["salary"] = df["salary"].fillna(df["salary"].median())

    df["phone"] = df["phone"].fillna("未知")

    null_after = df.isnull().sum().sum()
    logger.info(f"缺失值处理：{null_before} → {null_after}")

    # --- 2.3 异常值修复 ---
    # 负薪资 → 取绝对值
    negative_count = (df["salary"] < 0).sum()
    if negative_count > 0:
        logger.warning(f"发现 {negative_count} 条负薪资，已取绝对值")
        df.loc[df["salary"] < 0, "salary"] = abs(df.loc[df["salary"] < 0, "salary"])

    # 手机号格式校验
    invalid_phone = (df["phone"] != "未知") & ~df["phone"].str.match(
        r"^1\d{10}$", na=False
    )
    if invalid_phone.sum() > 0:
        logger.warning(f"发现 {invalid_phone.sum()} 个无效手机号，已标记为'无效号码'")
        df.loc[invalid_phone, "phone"] = "无效号码"

    # --- 2.4 日期标准化 ---
    df["hire_date"] = pd.to_datetime(df["hire_date"], errors="coerce")
    bad_dates = df["hire_date"].isna().sum()
    if bad_dates > 0:
        logger.warning(f"发现 {bad_dates} 个无效日期，已填为 NaT")
        df["hire_date"] = df["hire_date"].fillna(pd.NaT)

    logger.info(f"清洗完成，产出 {len(df)} 行 × {len(df.columns)} 列")
    return df


def generate_summary(df: pd.DataFrame) -> dict:
    """
    生成数据摘要统计，供写入报告。
    """
    summary = {
        "总人数": len(df),
        "部门数": df["department"].nunique(),
        "平均薪资": round(df["salary"].mean(), 0),
        "最高薪资": df["salary"].max(),
        "最低薪资": df["salary"].min(),
        "薪资中位数": df["salary"].median(),
        "缺失姓名": df["name"].isnull().sum(),
        "待分配部门": (df["department"] == "待分配").sum(),
        "无效手机号": (df["phone"] == "无效号码").sum(),
        "列信息": df.dtypes.to_dict(),
    }
    return summary

# =============================================================================
# 第 3 步：命令行参数解析
# =============================================================================

def parse_args():
    """
    定义脚本接受的命令行参数。
    argparse 自动生成 --help 文档。
    """
    parser = argparse.ArgumentParser(
        description="员工数据 ETL 流水线：读 → 洗 → 存 → 出报告",
        epilog="示例：python %(prog)s --input dirty.csv --output clean.csv --db --report report.txt",
    )

    parser.add_argument(
        "--input", "-i",
        required=True,
        help="输入 CSV 文件路径（必需）",
    )
    parser.add_argument(
        "--output", "-o",
        default="data_clean.csv",
        help="输出 CSV 文件路径（默认：data_clean.csv）",
    )
    parser.add_argument(
        "--db",
        action="store_true",   # 出现就是 True，不出现就是 False
        help="是否写入 MySQL employees 数据库",
    )
    parser.add_argument(
        "--report", "-r",
        default=None,
        help="文本报告输出路径（可选，如：report.txt）",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="显示 DEBUG 级别日志",
    )

    return parser.parse_args()

# =============================================================================
# 第 4 步：主流程
# =============================================================================

def main():
    """ETL 流水线入口。"""
    args = parse_args()

    # 如果用户加了 --verbose，显示 DEBUG 日志
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("=" * 50)
    logger.info("ETL 流水线启动")
    logger.info(f"输入文件: {args.input}")
    logger.info(f"输出文件: {args.output}")
    logger.info("=" * 50)

    # ── Step 1：读取 ──
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"文件不存在：{input_path.absolute()}")
        sys.exit(1)

    try:
        df = pd.read_csv(input_path)
        logger.info(f"✅ 读取完成：{len(df)} 行")
    except Exception as e:
        logger.error(f"读取失败：{e}")
        sys.exit(1)

    # ── Step 2：清洗 ──
    try:
        df_clean = clean_employee_data(df)
        logger.info("✅ 清洗完成")
    except Exception as e:
        logger.error(f"清洗失败：{e}")
        sys.exit(1)

    # ── Step 3：存为 CSV ──
    try:
        df_clean.to_csv(args.output, index=False, encoding="utf-8-sig")
        logger.info(f"✅ 已保存 CSV：{args.output}")
    except Exception as e:
        logger.error(f"保存 CSV 失败：{e}")
        sys.exit(1)

    # ── Step 4（可选）：写入 MySQL ──
    if args.db:
        try:
            engine = create_engine(
                "mysql+pymysql://root:@127.0.0.1:3306/employees?charset=utf8mb4"
            )
            df_clean.to_sql(
                "etl_employees_clean",
                engine,
                if_exists="replace",
                index=False,
            )
            logger.info("✅ 已写入数据库 employees.etl_employees_clean")
        except Exception as e:
            logger.error(f"数据库写入失败：{e}")
            # 数据库挂了不影响 CSV 产出，不 exit

    # ── Step 5（可选）：生成文本报告 ──
    if args.report:
        try:
            summary = generate_summary(df_clean)
            dept_stats = (
                df_clean.groupby("department")
                .agg(人数=("emp_id", "count"), 平均薪资=("salary", "mean"))
                .round(0)
            )

            with open(args.report, "w", encoding="utf-8") as f:
                f.write("员工数据清洗报告\n")
                f.write("=" * 40 + "\n\n")
                f.write("【数据概览】\n")
                for key, value in summary.items():
                    if key != "列信息":
                        f.write(f"  {key}: {value}\n")

                f.write("\n【部门统计】\n")
                f.write(dept_stats.to_string())
                f.write("\n\n【列类型】\n")
                for col, dtype in summary["列信息"].items():
                    f.write(f"  {col}: {dtype}\n")

            logger.info(f"✅ 报告已生成：{args.report}")
        except Exception as e:
            logger.error(f"报告生成失败：{e}")

    logger.info("=" * 50)
    logger.info("🎉 ETL 流水线完成")
    logger.info("=" * 50)

# =============================================================================
# 入口
# =============================================================================

if __name__ == "__main__":
    main()
    # 这一行的意思是：
    #   直接运行脚本 → 执行 main()
    #   被 import → 不执行 main()，只加载函数定义
    #
    # 这样这个脚本既可以当命令行工具用，
    # 也可以被其他脚本 import 复用里面的函数。
