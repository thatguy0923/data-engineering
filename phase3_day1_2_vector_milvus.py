"""
============================================================
Phase 3 · Day 1-2  向量化 + Milvus 入门
============================================================
目标：把「文本 → 向量 → 存进 Milvus → 语义检索」整条路跑通。
这是整个 Phase 3 语料检索项目的"心脏"，先在小数据上理解原理，
后面 Day 3+ 再灌 Tatoeba 真实日/韩语料。

【本节要建立的核心直觉】
  - 传统检索：按关键词"字面"匹配（LIKE '%词%'），换个说法就搜不到
  - 语义检索：把句子变成一串数字（向量），"意思相近"的句子向量也相近
             → 输入"我很开心"，能搜出"今天心情不错"，哪怕一个字都不重合
  - 向量库(Milvus)：专门存这些向量、并能极快地找出"最相近的 K 个"

【运行方式】
  cd ~/de-venv && source bin/activate
  python phase3_day1_2_vector_milvus.py

【本节规则】
  下面每个 TODO 你自己写，卡住先看 提示，别急着问我。
  写完一段就跑一次，看输出对不对，再往下写。
============================================================
"""

# ⚠️ 必须在 import 任何 HuggingFace 相关库之前设置！
# 国内直连 huggingface.co 会超时，走镜像站 hf-mirror.com 下载模型。
import os
os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"

from sentence_transformers import SentenceTransformer
from pymilvus import MilvusClient
import numpy as np


# ============================================================
# 第 0 步（已给你，不用改）：加载本地向量模型
# ------------------------------------------------------------
# 我们用多语言模型 paraphrase-multilingual-MiniLM-L12-v2：
#   - 支持 50+ 语言，含【日语、韩语、中文、英语】→ 正好符合 LingSnap 需求
#   - 输出 384 维向量
#   - 本地运行、不调 API → 免费 + 可离线（这是你简历里的"技术权衡"亮点）
#
# ⚠️ 第一次运行会自动下载模型（约 470MB），下载一次以后就缓存在本地了。
#    下载卡住是网络问题，不是代码问题。
# ============================================================
print("加载模型中（第一次会下载 ~470MB）...")
model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
print("模型加载完成 ✅\n")


# ============================================================
# TODO 1【文本 → 向量】把句子编码成向量，看看它长什么样
# ------------------------------------------------------------
# 要求：
#   1) 准备一句话，比如 sentence = "今天天气真好"
#   2) 用 model.encode(sentence) 得到向量，存进变量 vec
#   3) 打印：向量的类型、shape（维度）、以及前 5 个数字
#
# 你会看到：一句话变成了 384 个浮点数。这就是"向量化/embedding"。
#
# 提示：
#   - model.encode("字符串") 返回一个 numpy 数组
#   - 看维度用 vec.shape，看前几个数用 vec[:5]
# 你的答案：
sentence = "今天天气真好"
vec = model.encode(sentence)
print("向量类型:", type(vec))
print("向量形状:", vec.shape)
print("前 5 个数字:", vec[:5])


# ============================================================
# TODO 2【一次编码多句 + 理解"相似度"】
# ------------------------------------------------------------
# 语义检索的本质 = 比较两个向量有多"像"。最常用的度量是【余弦相似度】：
#   - 结果范围大致 -1 ~ 1，越接近 1 表示两句意思越接近
#
# 要求：
#   1) 准备一个句子列表 sentences，放 4 句，故意设计成"两两相关"，例如：
#         "我今天很开心"
#         "今天心情特别好"      ← 和上句意思接近，但用词不同
#         "这家餐厅的菜很难吃"
#         "这里的食物味道糟糕"   ← 和上句意思接近
#   2) 一次性编码：vecs = model.encode(sentences)  →  shape 应该是 (4, 384)
#   3) 写一个函数 cosine(a, b) 计算两个向量的余弦相似度
#         公式：dot(a,b) / (norm(a) * norm(b))
#         用 np.dot(a,b) 和 np.linalg.norm(a)
#   4) 打印：
#         - 句0 vs 句1 的相似度（应该偏高）
#         - 句0 vs 句2 的相似度（应该偏低）
#      验证："意思近的分高，意思远的分低"
#
# 提示：
#   - model.encode(列表) 直接返回 (N, 384) 的二维数组，vecs[0] 就是第一句的向量
#   - 不用自己实现 norm，np.linalg.norm 就是求模长
# 你的答案：
sentences = [
    "我今天很开心",
    "今天心情特别好",
    "这家餐厅的菜很难吃",
    "这里的食物味道糟糕"
]
vecs = model.encode(sentences)
cosine = lambda a, b: np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
print("句0 vs 句1 的相似度:", cosine(vecs[0], vecs[1]))
print("句0 vs 句2 的相似度:", cosine(vecs[0], vecs[2]))


# ============================================================
# TODO 3【连接 Milvus + 建集合(collection)】
# ------------------------------------------------------------
# 现在把向量存进 Milvus。学习阶段用 milvus-lite：一个本地文件就是一个库，
# 不用开 Docker。生产环境才换成 Milvus standalone（面试可讲这个权衡）。
#
# 概念对照（拿你熟的 MySQL 类比）：
#   MySQL              Milvus
#   ---------------    ----------------
#   数据库文件          MilvusClient("xxx.db")
#   表(table)          集合(collection)
#   行(row)            实体(entity)
#   列(column)         字段(field)
#   建索引加速查询       向量索引（Milvus 自动/可配）
#
# 要求：
#   1) client = MilvusClient("phase3_demo.db")   # 会在当前目录生成这个文件
#   2) 集合名 collection_name = "sentences_demo"
#   3) 如果集合已存在，先删掉（方便你反复重跑）：
#         if client.has_collection(collection_name):
#             client.drop_collection(collection_name)
#   4) 创建集合，指定向量维度 dimension=384（必须和模型输出维度一致！）
#         client.create_collection(collection_name=..., dimension=384)
#
# 提示：
#   - 维度写错（比如写 512）后面插入会报错，牢记"模型多少维，集合就多少维"
#   - MilvusClient 这种简化用法会自动建好主键 id 和向量字段 vector
# 你的答案：
client = MilvusClient("phase3_demo.db")
collection_name = "sentences_demo"
if client.has_collection(collection_name):
    client.drop_collection(collection_name)
client.create_collection(collection_name=collection_name, dimension=384)



# ============================================================
# TODO 4【把句子向量插入 Milvus】
# ------------------------------------------------------------
# Milvus 里每条数据(实体)是一个字典。用 MilvusClient 简化模式，最少要有：
#   - "id"      : 主键（整数，自己给 0,1,2...）
#   - "vector"  : 384 维向量（用 TODO 2 编码出的 vecs）
#   - 其它字段可选，比如把原文也存进去方便看：  "text": 原句
#
# 要求：
#   1) 把 TODO 2 的 sentences / vecs 组装成一个 data 列表，形如：
#         data = [
#             {"id": 0, "vector": vecs[0], "text": sentences[0]},
#             {"id": 1, "vector": vecs[1], "text": sentences[1]},
#             ...
#         ]
#      （提示：用 enumerate + 列表推导式一行搞定，别手写4行）
#   2) client.insert(collection_name=..., data=data)
#   3) 打印插入了多少条
#
# 提示：
#   - vecs[i] 是 numpy 数组，Milvus 能直接吃；若报类型错就 .tolist()
#   - 列表推导：[{"id": i, "vector": v, "text": s} for i,(v,s) in enumerate(zip(vecs, sentences))]
# 你的答案：
data = [{"id": i, "vector": v, "text": s} for i, (v, s) in enumerate(zip(vecs, sentences))]
client.insert(collection_name = collection_name, data = data)
print(f"插入了 {len(data)} 条数据")


# ============================================================
# TODO 5【语义检索 —— 本节高潮】
# ------------------------------------------------------------
# 现在做真正的事：给一句"查询词"，找出库里最相近的句子。
# 关键：查询词也要先用【同一个模型】编码成向量，才能和库里的比。
#
# 要求：
#   1) query = "心情很愉快"     # 故意不和任何库里句子用词重合
#   2) 把 query 编码成向量 query_vec（注意：search 要的是"向量列表"，
#      所以传 [query_vec] 或 model.encode([query])）
#   3) client.search(
#          collection_name=...,
#          data=[query_vec],          # 查询向量（列表）
#          limit=3,                   # 返回最相近的 3 条
#          output_fields=["text"],    # 让结果带上原文，方便看
#      )
#   4) 遍历结果打印：每条的 text 和 distance（相似度/距离分数）
#
# 你应该看到：库里"我今天很开心 / 今天心情特别好"排在最前面，
# 哪怕 query "心情很愉快" 和它们几乎没有相同的字 —— 这就是语义检索的威力。
#
# 提示：
#   - search 返回的是"每个查询向量对应一组结果"的嵌套结构：results[0] 才是第一条查询的命中列表
#   - 每个命中是字典，取 hit["entity"]["text"] 和 hit["distance"]
# 你的答案：
query= "心情很愉快"
query_vec = model.encode(query)
results = client.search(
    collection_name=collection_name,
    data=[query_vec],
    limit=3,
    output_fields=["text"]
)
print(f"查询词: {query}")
for hit in results[0]:
    print(f"命中: {hit['entity']['text']}, 相似度分数: {hit['distance']}")

# ============================================================
# TODO 6【回顾理解题 —— 写成注释即可，不用写代码】
# ------------------------------------------------------------
# 用自己的话回答（写在下面注释里），检验你真的懂了：
#
#   6-1) 为什么"查询词"必须用和入库时"同一个模型"来编码？
#        换个模型编码会怎样？
#
#   6-2) 语义检索 相比 传统 LIKE 关键词检索，好在哪？各自的短板是什么？
#
#   6-3) 我们这里用本地模型(不调 API)，对 LingSnap 这个产品有什么实际好处？
#        （提示：成本 / 离线 / 隐私 —— 这就是你面试要讲的"技术方案权衡"）
#
# 你的答案（写注释）：
#   6-1)确保编码一致，才能够正常查询，换一个模型导致编码不一致，查询结果会不准确。
#   6-2)语义检索可以查询相近意思的句子，而传统关键词检索只能查询与关键词完全匹配的句子，语义检索计算速度较慢，传统检索更快
#  - 语义检索短板:除了慢,还有要模型/要向量库、结果可能"太发散"(搜出来意思沾边但不是你要的精确词)
#  - 传统 LIKE 短板:换个说法就搜不到(搜"开心"永远搜不到"心情特别好")——这才是它最致命的
#  - 实际生产常常两个一起用(先关键词粗筛,再语义精排)
#   6-3)可以让产品在离线环境下运行，不需要付费节省成本，并且没有外来api接入，保护用户隐私


# ============================================================
# 全部写完后：
#   - 完整跑一遍，确认 TODO 5 能检索出"意思相近"的句子
#   - 跑完当前目录会多出 phase3_demo.db 文件（milvus-lite 的库），正常
#   - 然后叫我批改
# ============================================================
