import os
from collections import Counter

import polars as pl

# nltk.download("punkt")
from nltk.tokenize import word_tokenize
from tqdm import tqdm

from src import list_files_recursive


def txt_to_text(txt_file_path):
    # Open the HTML file
    with open(txt_file_path, "r", encoding="utf-8") as html_file:
        content = html_file.read()
    return content


def tokenize_text(text):
    return word_tokenize(text.lower())


def count_words(tokens):
    return Counter(tokens)


def relative_frequency(word_counts, total_words):
    return {word: (count / total_words) for word, count in word_counts.items()}


if __name__ == "__main__":
    curated = os.path.join("data", "curated")
    files = list_files_recursive(curated)
    staging = os.path.join("data", "staging")
    for file in tqdm(files):
        raw_filename = os.path.splitext(os.path.split(file)[1])[0]
        if not raw_filename.endswith("pu"):
            # skip non pts files for the moment
            continue
        vol_name = os.path.splitext(os.path.split(file)[-1])[0]

        text = txt_to_text(file)
        # risky new code - getting the English stuff at the start of pu
        text = text.split("htm")[-1]

        tokens = tokenize_text(text)
        word_counts = Counter(tokens)
        total_words = sum(word_counts.values())
        frequencies = relative_frequency(word_counts, total_words)
        words = list(frequencies.keys())
        frequencies = list(frequencies.values())

        lf = pl.LazyFrame({"words": words, "frequencies": frequencies}).with_columns(
            pl.lit(vol_name).alias("volume_name")
        )
        output_filename = (
            os.path.join(staging, file.lstrip(curated).rstrip(".txt")) + ".parquet"
        )
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        lf.sink_parquet(output_filename)
