"""RAG 检索 API 服务"""
import os
import numpy as np
import pandas as pd
import faiss
from fastapi import FastAPI, Query
from sentence_transformers import SentenceTransformer

app = FastAPI(title="RAG Search API", version="0.1.0")

# ─── 启动时加载（只加载一次，常驻内存） ───
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VECTORS_PATH = os.path.join(PROJECT_ROOT, "data/emb/vectors.npy")
META_PATH = os.path.join(PROJECT_ROOT, "data/emb/meta.parquet")
MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"

vectors = np.load(VECTORS_PATH)
meta = pd.read_parquet(META_PATH)
model = SentenceTransformer(MODEL_NAME)

index = faiss.IndexFlatIP(vectors.shape[1])
index.add(vectors)

print(f"✅ 索引就绪：{index.ntotal} 条向量，维度 {vectors.shape[1]}")


@app.get("/")
def root():
    return {"message": "RAG 检索服务运行中", "index_size": index.ntotal}


@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/search")
def search(
    q: str = Query(..., description="搜索关键词"),
    top_k: int = Query(5, ge=1, le=20, description="返回条数"),
    lang: str = Query(None, description="语言筛选（jpn/kor），留空则不筛选"),
):
    """语义检索：输入词/句 → 返回相关例句"""
    query_vec = model.encode([q], normalize_embeddings=True).astype("float32")
    scores, ids = index.search(query_vec, top_k)

    results = []
    for s, i in zip(scores[0], ids[0]):
        item = {"text": meta["text"].iloc[i], "lang": meta["lang"].iloc[i], "score": round(float(s), 4)}
        if lang is None or item["lang"] == lang:
            results.append(item)

    return {"query": q, "results": results}