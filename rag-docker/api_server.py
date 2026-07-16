"""RAG 检索 API 服务"""
from fastapi import FastAPI

app = FastAPI(title="RAG Search API", version="0.1.0")


@app.get("/")
def root():
    """根路径——欢迎页"""
    return {"message": "RAG 检索服务运行中", "version": "0.1.0"}


@app.get("/health")
def health():
    """健康检查——Kubernetes / Docker 用这个判断服务是否活着"""
    return {"status": "ok"}

@app.get("/echo/{word}")
def echo_path(word: str):
    """路径参数：/echo/hello → {"echo": "hello"}"""
    return {"echo": word}


@app.get("/search")
def search(q: str, lang: str = "jpn"):
    """查询参数：/search?q=ありがとう&lang=jpn"""
    return {"query": q, "lang": lang, "results": []}