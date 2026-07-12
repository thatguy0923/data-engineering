"""
============================================================
Phase 3 · Day 3  数据采集：Tatoeba 日/韩例句落地
============================================================
目标：把真实的多语言例句从 Tatoeba 下载下来，落地到本地 raw 层。
这一步叫【采集/ingestion】，是整条数据管道的源头。
Day 1-2 我们用手写的 4 句玩具数据，今天换成【真实、海量、脏】的语料。

【数据源】Tatoeba —— 开放的多语言例句库，免费、真实、够脏
  日语：https://downloads.tatoeba.org/exports/per_language/jpn/jpn_sentences.tsv.bz2
  韩语：https://downloads.tatoeba.org/exports/per_language/kor/kor_sentences.tsv.bz2
  格式：3 列 TSV（tab 分隔），【没有表头】：
        句子ID  <tab>  语种码  <tab>  例句文本
        例：  5350   kor    뭔가 해보자!

【本节核心思想：分层存储（很重要）】
  数据工程把数据分层放：
     raw（原始层/bronze）   ← 今天：原样落地，一个字都不改，先存下来
     cleaned（清洗层/silver）← Day 4-5：去重/去噪/语种校验后的干净数据
     serving（应用层/gold）  ← 后面：向量化入库供检索
  原则：【采集阶段只管"原样存下来"，绝不在这步清洗】。
        为什么？原始数据要留档、可复现、能用 DVC 版本管理；清洗逻辑会改，
        改错了还能从 raw 重来。把清洗混进采集，出错就没法回溯了。

【运行方式】
  cd ~/de-venv && source bin/activate
  python phase3_day3_tatoeba_ingest.py

【本节规则】
  每个 TODO 自己写，卡住看提示。写一段跑一段。
============================================================
"""

import os
import urllib.request
import pandas as pd


# ============================================================
# 第 0 步（已给你，不用改）：配置数据源和落地目录
# ------------------------------------------------------------
SOURCES = {
    "jpn": "https://downloads.tatoeba.org/exports/per_language/jpn/jpn_sentences.tsv.bz2",
    "kor": "https://downloads.tatoeba.org/exports/per_language/kor/kor_sentences.tsv.bz2",
}
RAW_DIR = "data/raw"   # 原始层落地目录


# ============================================================
# TODO 1【下载函数：把两个语言的 bz2 文件下载到 data/raw/】
# ------------------------------------------------------------
# 要求：
#   1) 先确保 RAW_DIR 目录存在（不存在就创建）
#         提示：os.makedirs(RAW_DIR, exist_ok=True)
#   2) 写一个循环，遍历 SOURCES，把每个 url 下载到 data/raw/ 下
#         文件名建议用 f"{lang}_sentences.tsv.bz2"，拼路径用 os.path.join(RAW_DIR, 文件名)
#   3) 下载用：urllib.request.urlretrieve(url, 目标路径)
#   4) 加个小优化：如果目标文件已存在就跳过下载（别每次重跑都重下）
#         提示：if os.path.exists(路径): print("已存在，跳过"); continue
#   5) 每个下载完打印一下文件路径和大小
#         提示：os.path.getsize(路径) 返回字节数
#
# 注意：这里下载的是【压缩包 .bz2】，先原样存下来，不解压——落地就是原样落地。
# 你的答案：
os.makedirs(RAW_DIR, exist_ok=True)
for lang, url in SOURCES.items():
    filename = f"{lang}_sentences.tsv.bz2"
    filepath = os.path.join(RAW_DIR, filename)
    if os.path.exists(filepath):
        print(f"{filepath} 已存在，跳过下载")
        continue
    urllib.request.urlretrieve(url, filepath)
    size = os.path.getsize(filepath)
    print(f"下载完成: {filepath}, 大小: {size} 字节")



# ============================================================
# TODO 2【读进 DataFrame：pandas 直接读 bz2，不用手动解压】
# ------------------------------------------------------------
# 惊喜：pandas 能直接读 .bz2 压缩文件，不用你先解压！
#
# 要求：
#   1) 写个函数 load_lang(lang) → 返回该语言的 DataFrame
#   2) 用 pd.read_csv 读 data/raw/{lang}_sentences.tsv.bz2，关键参数：
#         sep="\t"              # TSV 是 tab 分隔
#         header=None           # ⚠️ 文件没有表头！不指定会把第一行数据当列名
#         names=["id","lang","text"]   # 手动给 3 列命名
#         compression="bz2"     # 告诉 pandas 这是 bz2 压缩（其实 pandas 看后缀也能自动认，显式写更稳）
#   3) 分别加载：df_jpn = load_lang("jpn")；df_kor = load_lang("kor")
#   4) 各打印 shape，确认行数（韩语约 1.5 万+，日语更多）
#
# 提示：
#   - header=None 这个参数【最容易忘】，忘了第一句例句就变成列名了，牢记"无表头要声明"
# 你的答案：

def load_lang(lang):
    filepath = os.path.join(RAW_DIR, f"{lang}_sentences.tsv.bz2")
    df = pd.read_csv(filepath, sep = "\t", header = None, names = ["id", "lang", "text"], compression = "bz2")
    return df
df_jpn = load_lang("jpn")
df_kor = load_lang("kor")

# ============================================================
# TODO 3【数据体检：看看真实数据有多脏】★采集后必做
# ------------------------------------------------------------
# 落地后先"体检"，搞清楚数据长啥样、脏在哪，为 Day 4-5 清洗做准备。
# 【只看，不改】——发现的问题记下来，清洗留到下一课。
#
# 对 df_jpn（或随便挑一个）做以下检查，每项打印结果：
#   1) 前 5 行长啥样            → df.head()
#   2) 每列缺失值数量           → df.isnull().sum()
#   3) 重复的例句有多少条        → df["text"].duplicated().sum()
#   4) lang 列是不是纯净         → df["lang"].value_counts()
#         （理论上日语文件应该全是 jpn，但真实数据可能混入别的语种码）
#   5) 例句长度分布              → df["text"].str.len().describe()
#         （看有没有 极短(1个字符) 或 极长(几百字符) 的异常例句）
#
# 写完观察输出，在下面注释里记下你发现的【脏点】（Day 4-5 要处理这些）：
#   发现的问题：
#     -有超长句和超短句，可能不适合当例句
#     -没有空值
# 你的答案：
print("=== 日语数据体检 ===")
print(df_jpn.head())
print(df_jpn.isnull().sum())
print(df_jpn["text"].duplicated().sum())
print(df_jpn["lang"].value_counts())
print(df_jpn["text"].str.len().describe())



# ============================================================
# TODO 4【合并 + 落地 raw 层：存成 parquet】
# ------------------------------------------------------------
# 把日、韩两个 DataFrame 合并成一个，落地保存。
#
# 要求：
#   1) 用 pd.concat([df_jpn, df_kor], ignore_index=True) 合并成 df_all
#   2) 打印合并后的总行数、和 lang 分布（df_all["lang"].value_counts()）
#   3) 保存到 data/raw/tatoeba_raw.parquet
#         df_all.to_parquet("data/raw/tatoeba_raw.parquet", index=False)
#   4) 打印保存成功 + 文件大小
#
# 为什么用 parquet 不用 csv？
#   - 列式存储、带类型、压缩率高、读得快 —— 数据工程的事实标准
#   - 后面 PySpark(Day 4-5) 读 parquet 也更快
# 提示：
#   - to_parquet 需要 pyarrow，装过 pandas 一般就有；报错就 pip install pyarrow
# 你的答案：
df_all = pd.concat([df_jpn, df_kor], ignore_index = True)

print(f"合并后的总行数: {len(df_all)}")
print(df_all["lang"].value_counts())
df_all.to_parquet("data/raw/tatoeba_raw.parquet", index=False)
size = os.path.getsize("data/raw/tatoeba_raw.parquet")
print(f"保存成功: data/raw/tatoeba_raw.parquet, 大小: {size} 字节")



# ============================================================
# TODO 5【回顾理解题 —— 写注释即可】
# ------------------------------------------------------------
#   5-1) 为什么"采集"这一步要【原样落地、不清洗】？清洗混进采集会有什么坏处？
#
#   5-2) 我们把数据分成 raw / cleaned / serving 三层，这样分层有什么好处？
#        （提示：可复现 / 出错能回溯 / 各层能独立用 DVC 版本管理）
#
#   5-3) 这一步和你之前学的东西怎么衔接？
#        （提示：落地的 data/raw/ 下一步用 PySpark 清洗、用 GX 校验、
#         用 Airflow 定时重跑、用 DVC 管版本 —— 全都复用得上）
#
# 你的答案（写注释）：
#   5-1)保留数据原始状态，便于后续清洗和版本管理，混进清洗会导致操作不可回溯，数据不可复现
#   5-2)方便版本管理，出错可以回溯，方便数据复现
#   5-3)这一步是在之前学的东西的数据来源部分，不再是自己创造数据，而是真正的去采集数据，之后的清洗、校验、工作流等操作都是可以复用在这之后的


# ============================================================
# 全部写完后：
#   - 完整跑一遍，确认 data/raw/ 下有 3 个文件：
#       jpn_sentences.tsv.bz2 / kor_sentences.tsv.bz2 / tatoeba_raw.parquet
#   - TODO 3 的"脏点"观察记下来了没
#   - 然后叫我批改
# ============================================================
