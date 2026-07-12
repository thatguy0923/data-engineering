"""
Phase 3 · 向量化步骤（稳定版，供 Airflow DAG 调用）
------------------------------------------------------------
读 cleaned 语料 → 本地模型编码 → 存 data/emb/vectors.npy + meta.parquet
（原来在 /tmp/build_vectors.py，会被系统清掉，搬到这里长期保存）

幂等保护：若 vectors.npy 已存在则跳过，除非设了环境变量 FORCE_REBUILD=1。
  —— 数据管道里"重复跑不应该出错/不做无谓重算"是个重要习惯（面试可讲）。
  —— 也让你测 Airflow 接线时不用每次干等 6 分钟编码。
"""
import os
os.environ["HF_HUB_OFFLINE"] = "1"
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer

OUT_VEC = "data/emb/vectors.npy"
OUT_META = "data/emb/meta.parquet"

if os.path.exists(OUT_VEC) and os.environ.get("FORCE_REBUILD") != "1":
    print(f"⏭️  {OUT_VEC} 已存在，跳过向量化（要强制重算设 FORCE_REBUILD=1）", flush=True)
else:
    df = pd.read_parquet("data/cleaned/tatoeba_clean.parquet").reset_index(drop=True)
    print("编码语料:", len(df), flush=True)
    model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
    vecs = model.encode(df["text"].tolist(), batch_size=64,
                        normalize_embeddings=True, show_progress_bar=False)
    os.makedirs("data/emb", exist_ok=True)
    np.save(OUT_VEC, vecs.astype("float32"))
    df[["id", "lang", "text"]].to_parquet(OUT_META, index=False)
    print("已保存", OUT_VEC, "形状:", vecs.shape, flush=True)
