"""RAG 管道主入口：按顺序串联四个步骤。"""
import subprocess
import sys
from pathlib import Path

STEPS = [
    "ingest.py",           # ① 下载 Tatoeba 日/韩例句 → data/raw/
    "clean_validate.py",   # ② PySpark 清洗 + GX 质量校验 → data/cleaned/
    "vectorize.py",        # ③ 本地模型向量化 → data/emb/
    "search.py",           # ④ FAISS 语义检索测试
]

if __name__ == "__main__":
    pipeline_dir = Path(__file__).parent
    for step in STEPS:
        print(f"\n{'='*60}")
        print(f"▶  Running: {step}")
        print(f"{'='*60}")
        result = subprocess.run(
            [sys.executable, str(pipeline_dir / step)],
            check=False
        )
        if result.returncode != 0:
            print(f"\n❌ {step} 失败（exit code {result.returncode}），管道中断")
            sys.exit(1)
    print("\n✅ 管道四步全部完成！")
