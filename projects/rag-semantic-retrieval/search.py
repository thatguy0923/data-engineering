"""FAISS 语义检索：加载缓存向量，对查询做语义 + 跨语言检索。

向量与元数据由 vectorize.py 预先生成（data/emb/）。
FAISS 只存向量并返回行号，文本/语种通过行号回查 meta。
IndexFlatIP + 归一化向量 = 余弦相似度（分数越大越相似）。
"""
import os
os.environ["HF_HUB_OFFLINE"] = "1"

import numpy as np
import pandas as pd
import faiss
from sentence_transformers import SentenceTransformer

vectors = np.load("data/emb/vectors.npy")          # (N, 384) 已归一化 float32
meta = pd.read_parquet("data/emb/meta.parquet")     # 第 i 行 ↔ vectors[i]
model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

index = faiss.IndexFlatIP(vectors.shape[1])
index.add(vectors)


def search_examples(query, top_k=5):
    """返回与 query 语义最相近的 top_k 条例句 [(text, lang, score), ...]。"""
    q = model.encode([query], normalize_embeddings=True).astype("float32")
    scores, ids = index.search(q, top_k)
    return [(meta["text"].iloc[i], meta["lang"].iloc[i], float(s))
            for s, i in zip(scores[0], ids[0])]


if __name__ == "__main__":
    for kw in ["勉強", "ありがとう", "사랑"]:
        print(f"\n=== {kw} ===")
        for text, lang, score in search_examples(kw):
            print(f"[{lang}] {score:.3f}  {text}")
