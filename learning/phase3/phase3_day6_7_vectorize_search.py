"""
============================================================
Phase 3 · Day 6-7  真实语料向量化 + 语义检索（项目核心高潮）
============================================================
目标：把 Day 4-5 洗净的 26 万句语料【向量化 → 存进 Milvus】，
      再做出一个【输入词/句 → 返回最相关例句】的语义检索功能。
这就是要接入 LingSnap 的那个能力，也是整个简历项目的"卖点"。
Day 1-2 你在 4 句玩具数据上跑通过全流程，今天换成真实 26 万句。

【输入】 data/cleaned/tatoeba_clean.parquet （Day 4-5，263,296 句）
【输出】 tatoeba.db （milvus-lite 向量库，可反复检索）

【两天分工】
  Day 6 = 建索引：TODO 1-4（读数据 → 向量化 → 建集合 → 批量入库）
  Day 7 = 用起来：TODO 5-7（检索函数 → 实测 → 理解题）

【运行方式】
  cd ~/de-venv && source bin/activate
  python phase3_day6_7_vectorize_search.py
  ⚠️ 全量编码约 6 分钟（CPU）。开发时可先抽样跑通（见 TODO 1）

【本节规则】
  每个 TODO 自己写，卡住看提示。Day 1-2 的 phase3_day1_2_vector_milvus.py
  是你最好的参考——结构一样，只是数据变真了。
============================================================
"""

import os
# 模型已在 Day 1-2 下载缓存，直接走本地、跳过联网检查（省得网络抽风）
os.environ["HF_HUB_OFFLINE"] = "1"

import pandas as pd
from sentence_transformers import SentenceTransformer
from pymilvus import MilvusClient


# ============================================================
# 第 0 步（已给你，不用改）：配置
# ============================================================
CLEANED_PATH = "data/cleaned/tatoeba_clean.parquet"
DB_PATH = "tatoeba.db"
COLLECTION = "tatoeba_sentences"

print("加载模型...")
model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
print("模型就绪 ✅\n")


# ============================================================
# ===================  Day 6：建索引  =======================
# ============================================================

# ============================================================
# TODO 1【读入 cleaned 语料】
# ------------------------------------------------------------
# 要求：
#   1) 用 pd.read_parquet(CLEANED_PATH) 读进 df（pandas 能直接读 Spark 写的 parquet 目录）
#   2) 打印行数、lang 分布（df["lang"].value_counts()）
#   3) 【开发建议】想快速跑通全流程时，先抽样：df = df.head(5000)
#      跑通没问题后，把这行删掉/注释掉，再跑全量（约 6 分钟）
#
# 提示：
#   - 列是 id / lang / text，和 Day 4-5 落地的一致
# 你的答案：
df = pd.read_parquet(CLEANED_PATH)
print(f"语料行数: {len(df)}")
print("语种分布：")
print(df["lang"].value_counts())



# ============================================================
# TODO 2【批量向量化】
# ------------------------------------------------------------
# 把 df["text"] 全部编码成向量。
# 要求：
#   1) texts = df["text"].tolist()
#   2) vectors = model.encode(
#          texts,
#          batch_size=64,              # 批量编码，比一句句快
#          show_progress_bar=True,     # 显示进度条（6分钟不至于干等）
#          normalize_embeddings=True,  # ⚠️ 归一化，配 COSINE 用（见下）
#      )
#   3) 打印 vectors 的 shape（应该是 (行数, 384)）
#
# 为什么 normalize_embeddings=True？
#   - 我们用 COSINE（余弦）度量比"方向"是否相近，不关心向量长度
#   - 归一化把所有向量拉成单位长度，让余弦比较更规范、稳定
# 你的答案：
texts = df["text"].tolist()
vectors = model.encode(
    texts,
    batch_size = 64,
    show_progress_bar = True,
    normalize_embeddings = True
)
print(f"向量 shape: {vectors.shape}")



# ============================================================
# TODO 3【建 Milvus 集合（COSINE 度量）】
# ------------------------------------------------------------
# 要求：
#   1) client = MilvusClient(DB_PATH)
#   2) 若集合已存在先删（方便重跑）：
#         if client.has_collection(COLLECTION): client.drop_collection(COLLECTION)
#   3) 建集合，这次显式指定 COSINE：
#         client.create_collection(
#             collection_name=COLLECTION,
#             dimension=384,
#             metric_type="COSINE",   # Day 1-2 没指定用了默认L2，这次明确用余弦
#         )
#
# 提示：
#   - MilvusClient 简化模式默认开"动态字段"，所以插入时可以带 lang/text 等额外字段，不用先定义 schema
#   - ⚠️ 实测：这版 milvus-lite 的 COSINE distance 是【越小越相似，0=完全相同】
#     （TODO 6 会让你亲手验证这个方向）
# 你的答案：
client = MilvusClient(DB_PATH)
if client.has_collection(COLLECTION):
    client.drop_collection(COLLECTION)
client.create_collection(
    collection_name = COLLECTION,
    dimension = 384,
    metric_type = "COSINE"
)



# ============================================================
# TODO 4【批量插入 Milvus】
# ------------------------------------------------------------
# 26 万条别一次性 insert（内存/超时风险），分批插。
# 要求：
#   1) 组装每条数据为字典：{"id": i, "vector": vectors[i], "lang": ..., "text": ...}
#   2) 分批插入，每批比如 2000 条：
#         BATCH = 2000
#         for start in range(0, len(df), BATCH):
#             end = start + BATCH
#             batch_data = [
#                 {"id": i, "vector": vectors[i],
#                  "lang": df["lang"].iloc[i], "text": df["text"].iloc[i]}
#                 for i in range(start, min(end, len(df)))
#             ]
#             client.insert(collection_name=COLLECTION, data=batch_data)
#   3) 打印总共插入了多少条
#
# 提示：
#   - range(0, N, BATCH) 就是 0, 2000, 4000... 的分批起点
#   - min(end, len(df)) 防止最后一批越界
# 你的答案：
BATCH = 2000
for start in range(0, len(df), BATCH):
    end = start + BATCH
    batch_data = [
        {"id": i, "vector": vectors[i], "lang": df["lang"].iloc[i], "text": df["text"].iloc[i]}
        for i in range(start, min(end, len(df)))
    ]
    client.insert(collection_name=COLLECTION, data=batch_data)

# ============================================================
# ===================  Day 7：语义检索  =====================
# ============================================================

# ============================================================
# TODO 5【语义检索函数 —— 项目核心】★
# ------------------------------------------------------------
# 写一个函数：输入一个词/句，返回最相关的例句。这就是要接入 LingSnap 的能力。
#
# 要求：写 search_examples(query, lang=None, top_k=5)：
#   1) 把 query 编码成向量（记得 normalize_embeddings=True，和入库时一致）
#         q_vec = model.encode([query], normalize_embeddings=True)
#   2) 可选按语种过滤：如果传了 lang，就只在该语种里搜
#         flt = f'lang == "{lang}"' if lang else ""
#   3) 检索：
#         results = client.search(
#             collection_name=COLLECTION,
#             data=[q_vec[0]],
#             limit=top_k,
#             output_fields=["text", "lang"],
#             filter=flt,
#         )
#   4) 把结果整理成 [(text, distance), ...] 返回，并打印出来
#
# 提示：
#   - 结果嵌套：results[0] 才是命中列表；每个 hit 取 hit["entity"]["text"] 和 hit["distance"]
#   - filter 传空字符串 "" 表示不过滤（搜全部语种）
# 你的答案：
def search_examples(query, lang=None, top_k=5):
    q_vec = model.encode([query], normalize_embeddings = True)
    flt = f'lang == "{lang}"' if lang else ""
    results = client.search(
        collection_name = COLLECTION,
        data = [q_vec[0]],
        limit = top_k,
        output_fields = ["text", "lang"],
        filter = flt
    )
    for hit in results[0]:
      print(f"命中: {hit['entity']['text']}, 语种: {hit['entity']['lang']}, 相似度分数: {hit['distance']}")
    return [(hit['entity']['text'], hit['distance']) for hit in results[0]]


# ============================================================
# TODO 6【实测 + 验证 distance 方向】
# ------------------------------------------------------------
# 要求：
#   1) 查几个词，看检索效果（挑你想查的日/韩词）：
#         search_examples("勉強", lang="jpn")      # 日语"学习"
#         search_examples("사랑", lang="kor")      # 韩语"爱"
#         search_examples("ありがとう", lang="jpn")  # 日语"谢谢"
#   2) 【验证 distance 方向】用一句"库里肯定有的原句"去查（比如 df["text"].iloc[0]），
#      它应该排第一。看它的 distance 是接近 0 还是接近 1？
#      → 由此确认"越小越相似"还是"越大越相似"（这就是判断度量方向的通用技巧）
#
# 在下面注释记下你的观察：
#   distance 方向：
# 你的答案：
search_examples("勉強", lang="jpn")
search_examples("사랑", lang="kor")
search_examples("ありがとう", lang="jpn")
search_examples(df["text"].iloc[0])




# ============================================================
# TODO 7【理解题 —— 写注释】
# ------------------------------------------------------------
#   7-1) 入库时和查询时都要 normalize_embeddings=True，为什么必须"两边一致"？
#
#   7-2) 你怎么判断出 COSINE 的 distance 是越大还是越小更相似的？
#        （提示：拿库里已有的原句去查这个技巧）
#
#   7-3) search_examples 的 lang 过滤，对 LingSnap 有什么实际意义？
#        （提示：用户查日语词，只想要日语例句，不想混韩语）
#
#   7-4) 这个检索能力，具体怎么接进 LingSnap？画一下数据流。
#        （提示：用户划词 → 插件把词发给这个检索服务 → 返回例句 → 展示）
#
# 你的答案（写注释）：
#   7-1)
#   7-2)
#   7-3)
#   7-4)


# ============================================================
# 全部写完后：
#   - 先抽样(TODO 1 的 head)跑通 → 再全量跑一次（约6分钟）建好完整索引
#   - 确认 search_examples 能查出合理的相关例句
#   - 生成 tatoeba.db（这就是可反复检索的向量库）
#   - 然后叫我批改
# ============================================================
