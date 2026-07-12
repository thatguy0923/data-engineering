"""Tatoeba 日/韩例句采集：下载 → 读入 → 合并 → 落地 raw 层。

采集阶段只做原样落地，不在此清洗（清洗见 clean_validate.py）。
raw 层保留原始数据，便于复现、回溯与 DVC 版本管理。
"""
import os
import urllib.request
import pandas as pd

SOURCES = {
    "jpn": "https://downloads.tatoeba.org/exports/per_language/jpn/jpn_sentences.tsv.bz2",
    "kor": "https://downloads.tatoeba.org/exports/per_language/kor/kor_sentences.tsv.bz2",
}
RAW_DIR = "data/raw"


def download():
    """下载各语言 bz2 到 raw 层，已存在则跳过。"""
    os.makedirs(RAW_DIR, exist_ok=True)
    for lang, url in SOURCES.items():
        filepath = os.path.join(RAW_DIR, f"{lang}_sentences.tsv.bz2")
        if os.path.exists(filepath):
            print(f"{filepath} 已存在，跳过下载")
            continue
        urllib.request.urlretrieve(url, filepath)
        print(f"下载完成: {filepath}, {os.path.getsize(filepath)} 字节")


def load_lang(lang):
    """读取单语言 bz2（3 列 TSV，无表头）为 DataFrame。"""
    filepath = os.path.join(RAW_DIR, f"{lang}_sentences.tsv.bz2")
    return pd.read_csv(filepath, sep="\t", header=None,
                       names=["id", "lang", "text"], compression="bz2")


def main():
    download()
    df_all = pd.concat([load_lang("jpn"), load_lang("kor")], ignore_index=True)
    print(f"合并后总行数: {len(df_all)}")
    print(df_all["lang"].value_counts())
    df_all.to_parquet(f"{RAW_DIR}/tatoeba_raw.parquet", index=False)
    print(f"已落地 {RAW_DIR}/tatoeba_raw.parquet")


if __name__ == "__main__":
    main()
