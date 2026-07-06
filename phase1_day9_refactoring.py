#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 1 Day 9-10: 代码重构 + 错误处理 + 日志
============================================

学习目标：
  1. 把 Day 7-8 的 ETL 脚本拆成多模块：配置 / 清洗 / 存储 / 报告
  2. 自定义异常 — 不用泛泛的 Exception，而用 DataLoadError / CleanError
  3. 错误处理分层次 — 哪些能恢复、哪些直接停
  4. 日志输出到文件 — FileHandler，保留排查痕迹
  5. 配置集中管理 — 路径、阈值、数据库连接信息抽离出来
  6. 类型注解 — 让人和 IDE 一眼看出函数参数和返回值

为什么这很重要？
  面试官看你的代码第一眼就知道你是不是"野路子"。
  生产级代码和探索代码的区别不在地基，在"围墙"——错误兜得住、日志查得清、配置改得动。
"""

import logging
import sys
from pathlib import Path
from typing import Optional, Dict

import pandas as pd
from sqlalchemy import create_engine

# =============================================================================
# 第 1 步：配置集中管理
# =============================================================================
# 之前把路径、阈值散落在代码各处，改一个值要翻遍全文。
# 集中到 Config 类里：一处修改，全局生效。

class Config:
    """全局配置，改参数只改这里。"""

    # 数据库
    DB_USER = "root"
    DB_PASSWORD = ""
    DB_HOST = "127.0.0.1"
    DB_PORT = 3306
    DB_NAME = "employees"

    # 文件路径
    DEFAULT_OUTPUT = "data_clean.csv"
    LOG_FILE = "etl_pipeline.log"

    # 清洗规则
    PHONE_REGEX = r"^1\d{10}$"
    SALARY_MIN = 0   # 低于此值视为异常

    # 日志
    LOG_LEVEL = logging.INFO

    @classmethod
    def get_db_url(cls) -> str:
        return (
            f"mysql+pymysql://{cls.DB_USER}:{cls.DB_PASSWORD}"
            f"@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}?charset=utf8mb4"
        )

# =============================================================================
# 第 2 步：自定义异常
# =============================================================================
# 不用泛泛的 Exception——每种异常有明确的名字，调用方可以选择性捕获。
# 面试官看到会认为你有工程意识。

class ETLException(Exception):
    """ETL 流水线统一异常基类。"""
    pass


class DataLoadError(ETLException):
    """数据加载失败：文件不存在、格式错误、编码问题。"""
    pass


class DataCleanError(ETLException):
    """清洗失败：类型不匹配、必需列缺失。"""
    pass


class DatabaseWriteError(ETLException):
    """数据库写入失败。"""
    pass


class ConfigError(ETLException):
    """配置错误：缺少必需参数。"""
    pass

# =============================================================================
# 第 3 步：日志 — 同时输出到控制台 + 文件
# =============================================================================

def setup_logging(verbose: bool = False) -> logging.Logger:
    """
    配置双通道日志：
      - StreamHandler：控制台（INFO 级别，给人看）
      - FileHandler：文件（DEBUG 级别，给事后排查）

    Verbose 模式下控制台也开 DEBUG。
    """
    level = logging.DEBUG if verbose else Config.LOG_LEVEL

    # 根 logger
    logger = logging.getLogger("etl")
    logger.setLevel(logging.DEBUG)  # 最低放行 DEBUG，由 handler 各自过滤
    logger.handlers.clear()         # 防止重复添加

    # 控制台 handler：给人看的，默认 INFO
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    ))
    logger.addHandler(console)

    # 文件 handler：记录所有 DEBUG，事后排查用
    file_handler = logging.FileHandler(Config.LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(filename)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logger.addHandler(file_handler)

    return logger

# =============================================================================
# 第 4 步：各模块拆分为独立函数
# =============================================================================

# ---- 4.1 数据加载模块 ----

def load_data(filepath: str, logger: logging.Logger) -> pd.DataFrame:
    """
    加载 CSV，统一处理常见问题。
    抛出 DataLoadError，调用方决定是重试还是退出。
    """
    path = Path(filepath)
    if not path.exists():
        raise DataLoadError(f"文件不存在：{path.absolute()}")

    if path.suffix.lower() != ".csv":
        raise DataLoadError(f"不支持的文件格式：{path.suffix}，仅支持 CSV")

    try:
        df = pd.read_csv(path)
    except UnicodeDecodeError:
        # 尝试其他编码
        logger.warning("UTF-8 解码失败，尝试 GBK...")
        try:
            df = pd.read_csv(path, encoding="gbk")
        except Exception as e:
            raise DataLoadError(f"编码解析失败：{e}")
    except Exception as e:
        raise DataLoadError(f"读取失败：{e}")

    if df.empty:
        raise DataLoadError(f"文件为空：{path}")

    logger.info(f"加载成功：{len(df)} 行 × {len(df.columns)} 列")
    logger.debug(f"列名：{list(df.columns)}")
    return df


# ---- 4.2 数据验证模块 ----

def validate_columns(df: pd.DataFrame, required: list, logger: logging.Logger) -> None:
    """检查必需列是否存在。缺失则抛出 DataCleanError。"""
    missing = set(required) - set(df.columns)
    if missing:
        raise DataCleanError(f"缺少必需列：{missing}")
    logger.debug(f"列校验通过，{len(required)} 个必需列全部存在")


# ---- 4.3 数据清洗模块 ----

def clean_duplicates(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """去重。"""
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    if removed > 0:
        logger.info(f"去重：移除 {removed} 行（{before} → {len(df)}）")
    return df


def clean_missing_values(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    缺失值处理。
    每一列有明确的填充策略，不是一刀切的 fillna(0)。
    """
    # 策略字典：列名 → 填充值或方法
    strategies: Dict[str, any] = {
        "name": lambda d: d["name"].fillna("员工_" + d["emp_id"].astype(str)),
        "department": "待分配",
        "phone": "未知",
        "manager": lambda d: d.groupby("department")["manager"].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "未知")
        ),
    }

    fixed_count = 0
    for col, strategy in strategies.items():
        if col not in df.columns:
            continue
        null_count = df[col].isnull().sum()
        if null_count == 0:
            continue
        if callable(strategy):
            df[col] = strategy(df)
        else:
            df[col] = df[col].fillna(strategy)
        fixed_count += null_count
        logger.debug(f"  {col}: 填充 {null_count} 个缺失值")

    # 薪资：分组中位数 → 全局中位数兜底
    if "salary" in df.columns and "department" in df.columns:
        null_salary = df["salary"].isnull().sum()
        if null_salary > 0:
            df["salary"] = df.groupby("department")["salary"].transform(
                lambda x: x.fillna(x.median())
            )
            df["salary"] = df["salary"].fillna(df["salary"].median())
            fixed_count += null_salary
            logger.debug(f"  salary: 填充 {null_salary} 个缺失值")

    logger.info(f"缺失值处理：修复 {fixed_count} 个")
    return df


def clean_outliers(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """异常值处理：负薪资、无效手机号、非法日期。"""
    issues = []

    # 薪资异常
    if "salary" in df.columns:
        negative = (df["salary"] < Config.SALARY_MIN).sum()
        if negative > 0:
            df.loc[df["salary"] < Config.SALARY_MIN, "salary"] = abs(
                df.loc[df["salary"] < Config.SALARY_MIN, "salary"]
            )
            issues.append(f"负薪资 {negative} 条（已取绝对值）")

        # 极端值检测：超过 3 倍标准差
        mean_s = df["salary"].mean()
        std_s = df["salary"].std()
        extreme = (df["salary"] > mean_s + 3 * std_s)
        if extreme.sum() > 0:
            logger.warning(f"发现 {extreme.sum()} 条极端薪资（> {mean_s + 3*std_s:.0f}）")

    # 手机号异常
    if "phone" in df.columns:
        has_phone = (df["phone"] != "未知") & df["phone"].notna()
        invalid = has_phone & ~df["phone"].str.match(Config.PHONE_REGEX, na=False)
        if invalid.sum() > 0:
            df.loc[invalid, "phone"] = "无效号码"
            issues.append(f"无效手机号 {invalid.sum()} 条")

    # 日期异常
    if "hire_date" in df.columns:
        df["hire_date"] = pd.to_datetime(df["hire_date"], errors="coerce")
        bad = df["hire_date"].isna().sum()
        if bad > 0:
            issues.append(f"无效日期 {bad} 条（已置为 NaT）")

    if issues:
        logger.warning("异常数据处理：" + "；".join(issues))
    return df


def clean_data(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    完整清洗流程，按顺序执行各步骤。
    每步独立，出问题时能定位到具体环节。
    """
    logger.info("=" * 40)
    logger.info("开始数据清洗")

    # 先校验列
    required_cols = ["emp_id", "name", "salary", "hire_date"]
    validate_columns(df, required_cols, logger)

    df = clean_duplicates(df, logger)
    df = clean_missing_values(df, logger)
    df = clean_outliers(df, logger)

    logger.info(f"清洗完成：{len(df)} 行 × {len(df.columns)} 列")
    return df


# ---- 4.4 数据存储模块 ----

def save_to_csv(df: pd.DataFrame, path: str, logger: logging.Logger) -> None:
    """保存 CSV，创建父目录如果不存在。"""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    logger.info(f"已保存 CSV：{out.absolute()} ({len(df)} 行)")


def save_to_db(df: pd.DataFrame, table: str, logger: logging.Logger) -> None:
    """
    写入数据库。
    异常不中断流程——数据库挂了 CSV 还在，不丢数据。
    """
    try:
        engine = create_engine(Config.get_db_url())
        df.to_sql(table, engine, if_exists="replace", index=False)
        logger.info(f"已写入数据库：{Config.DB_NAME}.{table}")
    except Exception as e:
        raise DatabaseWriteError(f"数据库写入失败：{e}")


# ---- 4.5 报告生成模块 ----

def generate_report(df: pd.DataFrame, output: str, logger: logging.Logger) -> None:
    """生成文本摘要报告。"""
    lines = [
        "数据清洗报告",
        "=" * 40,
        "",
        f"总行数: {len(df)}",
        f"总列数: {len(df.columns)}",
        f"列名: {', '.join(df.columns)}",
    ]

    if "salary" in df.columns:
        lines += [
            "",
            "【薪资统计】",
            f"  平均值: {df['salary'].mean():.0f}",
            f"  中位数: {df['salary'].median():.0f}",
            f"  最高: {df['salary'].max()}",
            f"  最低: {df['salary'].min()}",
        ]

    if "department" in df.columns:
        lines += [
            "",
            "【部门分布】",
        ]
        for dept, count in df["department"].value_counts().items():
            lines.append(f"  {dept}: {count} 人")

    Path(output).write_text("\n".join(lines), encoding="utf-8")
    logger.info(f"已生成报告：{output}")

# =============================================================================
# 第 5 步：主流程 — 错误分级处理
# =============================================================================
# 核心原则：
#   致命错误 → 直接退出（文件不存在、列缺失）
#   可恢复错误 → 记录日志继续跑（数据库挂了，CSV 还在）
#   所有异常都进日志文件，事后能回溯

def main():
    # 先用 argparse 解析参数
    import argparse
    parser = argparse.ArgumentParser(description="ETL 数据清洗流水线（重构版）")
    parser.add_argument("--input", "-i", required=True, help="输入 CSV")
    parser.add_argument("--output", "-o", default=Config.DEFAULT_OUTPUT, help="输出 CSV")
    parser.add_argument("--db", action="store_true", help="写入数据库")
    parser.add_argument("--report", "-r", help="报告输出路径")
    parser.add_argument("--verbose", "-v", action="store_true", help="DEBUG 日志")
    args = parser.parse_args()

    # 一口气传参，不用后面再手动补
    logger = setup_logging(verbose=args.verbose)

    logger.info("ETL 流水线启动（重构版）")
    logger.info(f"输入={args.input}  输出={args.output}  入库={'是' if args.db else '否'}")

    # ---- 致命错误：直接退出 ----
    fatal_errors = (DataLoadError, DataCleanError, ConfigError)

    # 加载
    try:
        df = load_data(args.input, logger)
    except fatal_errors as e:
        logger.error(f"致命错误：{e}")
        sys.exit(1)

    # 清洗
    try:
        df = clean_data(df, logger)
    except fatal_errors as e:
        logger.error(f"清洗失败：{e}")
        sys.exit(1)

    # 保存 CSV（必须成功）
    try:
        save_to_csv(df, args.output, logger)
    except Exception as e:
        logger.error(f"保存 CSV 失败：{e}")
        sys.exit(1)

    # ---- 非致命：数据库挂了信号异常，不中断 ----
    if args.db:
        try:
            save_to_db(df, "etl_employees_clean_v2", logger)
        except DatabaseWriteError as e:
            logger.warning(f"入库跳过：{e}")

    # 报告
    if args.report:
        try:
            generate_report(df, args.report, logger)
        except Exception as e:
            logger.warning(f"报告生成失败：{e}")

    logger.info("ETL 流水线完成 🎉")

# =============================================================================
# 入口
# =============================================================================

if __name__ == "__main__":
    main()

# =============================================================================
# 核心知识点总结
# =============================================================================
"""
┌──────────────────────┬────────────────────────────────────────────┐
│ 概念                  │ 作用                                       │
├──────────────────────┼────────────────────────────────────────────┤
│ 自定义异常             │ DataLoadError / CleanError … 调用方可选择性捕获 │
│ 配置类 Config          │ 路径/阈值/连接信息集中管理                   │
│ 双通道日志             │ 控制台给人看 + 文件给排查用                  │
│ FileHandler           │ 日志写进文件，关了终端也能查                  │
│ 策略字典 strategies    │ 每列填充策略显式声明，比一堆 if 清晰          │
│ try/except 分级        │ 致命错误 exit(1) / 非致命 warn 继续跑        │
│ 类型注解 →             │ def load(file: str, log: Logger) -> DataFrame │
│ callable() 判断        │ 判断一个值是函数还是普通值                   │
└──────────────────────┴────────────────────────────────────────────┘

面试可能问到：
  Q: 为什么自定义异常而不是用 Exception？
  A: 调用方可以按异常类型决定行为。DataLoadError → 退出；
     DatabaseWriteError → 记录日志继续。泛泛的 Exception 没法区分。

  Q: 为什么日志要同时写控制台和文件？
  A: 控制台是实时监控用的（INFO 就够了），文件是出事之后排
     查用的（DEBUG 全量保留，含文件名+行号）。控制台关了日志还在。

  Q: Config 类的好处是什么？
  A: 路径/阈值/参数集中管理，改一处全局生效；面试官觉得你做过
     真项目——硬编码是 Jupyter 习惯，Config 是工程习惯。
"""
