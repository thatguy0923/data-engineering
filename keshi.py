"""可视化 Tatoeba 语料数据"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 用你 Docker 管道产出的真实数据
df = pd.read_parquet("data/cleaned/tatoeba_clean.parquet")

# 设置中文字体（Mac 用这个）
plt.rcParams["font.sans-serif"] = ["Arial Unicode MS"]

# ─── 图1：语种分布（柱状图） ───
plt.figure(figsize=(8, 4))
df["lang"].value_counts().plot(kind="bar", color=["#FF6B6B", "#4ECDC4"])
plt.title("语种分布")
plt.xlabel("语言")
plt.ylabel("句子数量")
plt.tight_layout()
plt.savefig("output/01_lang_dist.png", dpi=150)
print("图1 已保存")

# ─── 图2：句子长度分布（直方图） ───
plt.figure(figsize=(8, 4))
df["text_len"] = df["text"].str.len()
df["text_len"].hist(bins=50, color="#6C5CE7", edgecolor="white")
plt.axvline(df["text_len"].mean(), color="red", linestyle="--", label=f'均值={df["text_len"].mean():.0f}')
plt.title("句子长度分布")
plt.xlabel("字符数")
plt.ylabel("句子数量")
plt.legend()
plt.tight_layout()
plt.savefig("output/02_length_dist.png", dpi=150)
print("图2 已保存")

# ─── 图3：按语种分组的长度箱线图 ───
plt.figure(figsize=(8, 4))
sns.boxplot(data=df, x="lang", y="text_len", palette=["#FF6B6B", "#4ECDC4"])
plt.title("句子长度对比（日 vs 韩）")
plt.xlabel("语言")
plt.ylabel("字符数")
plt.tight_layout()
plt.savefig("output/03_boxplot.png", dpi=150)
print("图3 已保存")

# ─── 图4：句子长度 Top 20（水平柱状图） ───
plt.figure(figsize=(8, 6))
top20 = df.nlargest(20, "text_len")[["text", "text_len", "lang"]]
colors = ["#FF6B6B" if l == "jpn" else "#4ECDC4" for l in top20["lang"]]
plt.barh(range(20), top20["text_len"], color=colors)
plt.yticks(range(20), [t[:30] + "..." for t in top20["text"]], fontsize=7)
plt.title("最长的 20 个句子")
plt.xlabel("字符数")
plt.tight_layout()
plt.savefig("output/04_top20_longest.png", dpi=150)
print("图4 已保存")