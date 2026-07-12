"""
============================================================
Phase 3 · Day 6-7  语义检索（FAISS 版）
============================================================
背景：milvus-lite 在 26 万句规模上不稳定（torch + 原生库在同进程抢资源崩溃）。
      改用 FAISS —— Facebook 出的向量检索库，纯库、零服务、极稳，
      而且它就是很多向量数据库（含 Milvus）底层用的引擎，学它最实在。

【和 Milvus 最大的区别（重要）】
  Milvus：存向量，也帮你存 text/lang，检索直接返回原文
  FAISS ：【只存向量】，返回的是"第几行"（整数下标），
          text/lang 要【你自己拿下标去 meta 表里查】
  → FAISS 更底层、更纯粹：它只干"最近邻搜索"这一件事，元数据你自己管

【已有材料（Day 6 已把编码结果存磁盘，本文件不再重编码）】
  data/emb/vectors.npy    26 万句的向量 (N, 384)，float32，已归一化
  data/emb/meta.parquet   对应的 id/lang/text（第 i 行 ↔ vectors[i]）

【运行方式】
  cd ~/de-venv && source bin/activate
  python phase3_day6_7_faiss_search.py
  （秒级，不再等 6 分钟——向量已在磁盘上）

【本节规则】每个 TODO 自己写，卡住看提示。
============================================================
"""

import os
os.environ["HF_HUB_OFFLINE"] = "1"

import numpy as np
import pandas as pd
import faiss
from sentence_transformers import SentenceTransformer


# ============================================================
# 第 0 步（已给你）：载入缓存好的向量 + 元数据 + 模型
# ------------------------------------------------------------
# 注意：这里不再 encode 26 万句，只从磁盘读现成的向量，所以很快。
# 模型还要加载，是因为【查询词】要现场编码（一次一句，瞬间完成）。
# ============================================================
vectors = np.load("data/emb/vectors.npy")          # (N, 384) 已归一化 float32
meta = pd.read_parquet("data/emb/meta.parquet")     # 第 i 行 ↔ vectors[i]
print("向量:", vectors.shape, "| 元数据:", len(meta), "行")

model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
print("模型就绪 ✅\n")


# ============================================================
# TODO 1【建 FAISS 索引 + 灌入向量】
# ------------------------------------------------------------
# FAISS 的"索引(index)"就相当于 Milvus 的集合。最基础的一种：
#   faiss.IndexFlatIP(dim)  —— IP = Inner Product 内积
#   为什么用 IP？因为我们的向量【已归一化】，归一化后【内积 == 余弦相似度】。
#   所以 IndexFlatIP + 归一化向量 = 余弦检索。
#
# 要求：
#   1) dim = vectors.shape[1]   # 384
#   2) index = faiss.IndexFlatIP(dim)
#   3) index.add(vectors)       # 把 26 万个向量灌进去
#   4) 打印 index.ntotal        # 索引里有多少向量，应等于 26 万+
#
# 提示：
#   - vectors 必须是 float32 的 numpy 数组（我们存的时候已经转好了）
#   - IndexFlat = 暴力精确搜索（逐个算），26 万这个量级足够快；
#     更大数据才需要 IVF/HNSW 这类近似索引（面试可提一句，这里用不上）
# 你的答案：
dim = vectors.shape[1]
index = faiss.IndexFlatIP(dim)
index.add(vectors)
print(index.ntotal)



# ============================================================
# TODO 2【语义检索函数 —— 核心】★
# ------------------------------------------------------------
# 写 search_examples(query, top_k=5)：输入词/句 → 返回最相关的例句。
#
# 要求：
#   1) 把 query 编码成向量，务必归一化（和入库时一致）：
#         q = model.encode([query], normalize_embeddings=True)  # 形状 (1, 384)
#         q = q.astype("float32")
#   2) 检索：
#         scores, ids = index.search(q, top_k)
#      说明：
#         - index.search 一次可查多条，所以传的是 (1, 384) 的二维数组
#         - 返回两个数组：scores(相似度分数) 和 ids(命中的行下标)
#         - 都是形状 (1, top_k)，所以取 scores[0]、ids[0]
#   3) 【关键】ids 是"第几行"，不是原文！用它去 meta 里查回文本：
#         for score, row_id in zip(scores[0], ids[0]):
#             text = meta["text"].iloc[row_id]
#             lang = meta["lang"].iloc[row_id]
#             print(...)
#   4) 返回 [(text, lang, score), ...]
#
# 提示：
#   - 这就是 FAISS 和 Milvus 的核心区别：FAISS 只还你下标，原文你自己用下标去 meta 查
#   - IndexFlatIP 的 score 是内积=余弦：【越大越相似】（和 milvus-lite 的 distance 相反！）
# 你的答案：
def search_examples(query, top_k = 5):
  q = model.encode([query], normalize_embeddings = True)
  q = q.astype("float32")
  scores, ids = index.search(q, top_k)
  for score, row_id in zip(scores[0], ids[0]):
    text = meta["text"].iloc[row_id]
    lang = meta["lang"].iloc[row_id]
    print(f"{text}; {lang}; {score}")
  return [(meta["text"].iloc[i], meta["lang"].iloc[i], s) for s, i in zip(scores[0], ids[0])]



# ============================================================
# TODO 3【实测 + 观察 score 方向】
# ------------------------------------------------------------
# 要求：
#   1) 查几个词看效果：
#         search_examples("勉強")       # 日语"学习"相关
#         search_examples("ありがとう")  # 日语"谢谢"
#         search_examples("사랑")        # 韩语"爱"
#   2) 验证方向：用一句库里已有的原句去查
#         search_examples(meta["text"].iloc[0])
#      它应该排第一，score 接近 1（因为 IndexFlatIP 是"越大越相似"，完全相同≈1.0）
#
# 在注释里记下观察：
#   score 方向： 都很接近1
# 你的答案：
search_examples("勉強")       # 日语"学习"相关
search_examples("ありがとう")  # 日语"谢谢"
search_examples("사랑")        # 韩语"爱"
search_examples(meta["text"].iloc[0])
# ============================================================
# TODO 4【理解题 —— 写注释】
# ------------------------------------------------------------
#   4-1) FAISS 和 Milvus 存的东西有什么不同？为什么 FAISS 检索完还要"拿下标去 meta 查"？
#
#   4-2) 为什么向量归一化后，用内积(IP)就等于余弦相似度？（想想余弦公式的分母）
#
#   4-3) IndexFlatIP 的 score 是越大还是越小更相似？你怎么验证的？
#
#   4-4) 这个 search_examples 怎么接进 LingSnap？画一下数据流。
#
# 你的答案（写注释）：
#   4-1)faiss存的是向量，milvus是数据库，什么都会存；因为faiss不存内容，存的是索引，所以得到索引之后需要去meta表里面自己查
#   4-2)分母是模的乘积，归一化之后，就等于一，所以两者就相等了
#   4-3)越大越相似，用语料库自身的数据进行查询，与自己的匹配度为1
#   4-4)lingsnap划词 >> 后端服务（把检索打包成api接入）>> 编码 >> search_example >> 拿到结果 >> 结果返回lingsnap


# ============================================================
# 全部写完后：
#   - 跑一遍（秒级），确认能查出合理的相关例句
#   - 然后叫我批改
# ============================================================
