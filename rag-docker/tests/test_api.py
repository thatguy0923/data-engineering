"""测试 API 端点——数据不存在时跳过，不炸 CI。"""
import os
import pytest
from fastapi.testclient import TestClient


# 检查数据文件是否存在（CI 里没有，会跳过）
DATA_READY = (
    os.path.exists("data/emb/vectors.npy") and
    os.path.exists("data/emb/meta.parquet")
)

# 只有数据就绪才导入了 api_server（导入时会加载模型到内存）
if DATA_READY:
    from api_server import app
    client = TestClient(app)


@pytest.mark.skipif(not DATA_READY, reason="无向量数据，跳过 API 测试")
class TestAPI:
    """需要真实数据才能跑的测试"""

    def test_root(self):
        resp = client.get("/")
        assert resp.status_code == 200
        data = resp.json()
        assert "message" in data
        assert "index_size" in data

    def test_health(self):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

    def test_search_returns_results(self):
        resp = client.get("/search?q=勉強&top_k=3")
        assert resp.status_code == 200
        data = resp.json()
        assert data["query"] == "勉強"
        assert len(data["results"]) > 0
        for r in data["results"]:
            assert "text" in r
            assert "lang" in r
            assert "score" in r

    def test_search_lang_filter(self):
        resp = client.get("/search?q=ありがとう&top_k=10&lang=jpn")
        assert resp.status_code == 200
        results = resp.json()["results"]
        # 过滤后应该全是日语
        assert all(r["lang"] == "jpn" for r in results)

    def test_invalid_top_k_rejected(self):
        """top_k > 20 应该被 FastAPI 校验拦截"""
        resp = client.get("/search?q=test&top_k=999")
        assert resp.status_code == 422  # FastAPI 参数校验失败


def test_app_module_exists():
    """即使没有数据，api_server.py 文件存在且语法正确。"""
    import ast
    with open("api_server.py", "r") as f:
        tree = ast.parse(f.read())
    # 检查文件能被 Python 正确解析（非空、语法正确）
    assert len(tree.body) > 0, "api_server.py 是空文件"
