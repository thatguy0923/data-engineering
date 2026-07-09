# 数据工程学习与实践

从零搭建生产级数据管道的学习项目，覆盖 ETL 开发、分布式计算、工作流调度、数据质量与版本管理。核心是一套完整的**电商退款分析数据管道**。

## 技术栈

| 类别 | 技术 |
|------|------|
| 数据处理 | Pandas、NumPy、PySpark |
| 存储 | MySQL（SQLAlchemy / JDBC） |
| 工作流调度 | Apache Airflow |
| 数据质量 | Great Expectations |
| 版本管理 | Git、DVC |
| 语言 | Python 3.12、SQL |

## 核心项目：电商退款分析 ETL 管道

一条端到端的生产级数据管道，每日分析电商退款数据（退款品类分布、异常店铺、退款率）。

**数据流程：**

```
CSV 原始数据 → PySpark 清洗 → 聚合计算 → MySQL → 数据质量验证 → Airflow 调度
```

**关键实现：**

- **分布式 ETL**：PySpark 完成清洗、聚合，支持大数据量
- **数据倾斜处理**：针对单店铺占比过高（40%）的倾斜问题，采用**加盐法（Salting）两阶段聚合**打散热点 key
- **数据入库**：PySpark JDBC 写入 MySQL 结果表
- **数据质量验证**：Great Expectations 声明式规则，自动校验并生成报告，异常时告警
- **自动化调度**：Airflow DAG 每日定时触发，支持失败重试与告警回调
- **工程规范**：配置隔离（Config 类）、自定义异常、双通道日志、命令行参数（argparse）

相关文件：`refund_etl.py`、`refund_config.py`、`capstone_project.py`、`airflow/dags/refund_pipeline.py`

## 项目结构

```
.
├── refund_etl.py              # 退款分析 ETL 主脚本（清洗/加盐聚合/入库/验证）
├── refund_config.py           # 配置类 + 自定义异常 + 日志
├── capstone_project.py        # 综合项目：电商退款分析系统
├── pipeline_etl.py            # 通用 ETL 管道脚本
├── dvc.yaml                   # DVC 管道定义（管道即代码）
├── airflow/dags/              # Airflow DAG 定义
│   └── refund_pipeline.py     # 退款管道调度（ETL → 质量验证）
├── data/                      # 数据生成脚本
├── phase1_*.py                # Python 数据栈（Pandas / NumPy / SQLAlchemy / ETL）
├── phase2_*.py                # PySpark / Airflow / Great Expectations / DVC
└── *.sql                      # SQL 练习（JOIN / 子查询 / CTE / 窗口函数 / 索引优化）
```

## 快速开始

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 生成测试数据
python data/generate_refund_data.py

# 3. 运行 ETL 管道（可分步执行）
python refund_etl.py --date 2025-06-01 --step all
python refund_etl.py --date 2025-06-01 --step extract   # 仅抽取
python refund_etl.py --date 2025-06-01 --step validate  # 仅验证

# 4. （可选）通过 Airflow 调度
#    将 airflow/dags/ 配置到 Airflow，DAG: refund_pipeline
```

> 运行 PySpark 需要 MySQL JDBC 驱动，并设置 `PYSPARK_PYTHON` 指向虚拟环境的 Python。

## 学习内容覆盖

- **SQL**：多表 JOIN、子查询、CTE、窗口函数、索引与 EXPLAIN 优化
- **Python 数据栈**：Pandas 清洗/聚合、NumPy 向量化、SQLAlchemy、命令行 ETL、代码重构
- **PySpark**：DataFrame API、分区与 Shuffle、数据倾斜与加盐法
- **Airflow**：DAG、Operator、任务依赖、重试、Sensor、Hook、告警
- **数据质量**：Great Expectations 声明式验证、Checkpoint、报告
- **版本管理**：DVC 数据版本控制、dvc.yaml 管道、远程存储
