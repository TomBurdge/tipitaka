import os
import re
import string
import zipfile

from bs4 import BeautifulSoup
from nltk import download
from nltk.corpus import stopwords
from tqdm import tqdm

from src import list_files_recursive

download("stopwords")


def unzip_file(zip_filepath, dest_dir):
    """The function `unzip_file` extracts all files from a given zip file and saves them to a specified destination directory.

    :param zip_filepath: The path to the zip file that you want to unzip
    :param dest_dir: The destination directory where the files from the zip file will be extracted to
    """
    with zipfile.ZipFile(zip_filepath, "r") as zip_ref:
        zip_ref.extractall(dest_dir)


def html_to_text(html_file_path):
    # Open the HTML file
    with open(html_file_path, "r", encoding="utf-8") as html_file:
        content = html_file.read()

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(content, "html.parser")

    # Extract text
    text = soup.get_text(strip=True)
    return text


def string_to_text(content):
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(content, "html.parser")

    # Extract text
    text = soup.get_text(strip=True)
    return text


def remove_patterns(s, substrings_to_remove=[]):
    s = re.sub(r"\n", " ", s)

    # Remove words which are all capitals
    s = re.sub(r"\b[A-Z]+\b", " ", s)

    s = s.lower()

    # Remove anything inside two brackets
    s = re.sub(r"\[.*?\]", " ", s)

    # Remove any number followed by a full stop
    s = re.sub(r"\d+\.", " ", s)

    # Remove numbers
    s = re.sub(r"\d+", " ", s)

    # remove punctuation
    s = re.sub(f"[{re.escape(string.punctuation)}]", " ", s)

    # Remove Roman numerals
    roman_pattern = r"\bM{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})\b"
    s = re.sub(roman_pattern, " ", s, flags=re.I)

    # remove Jataka + numerals
    ja_roman_pattern = r"\bjaM{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})\b"
    s = re.sub(ja_roman_pattern, " ", s, flags=re.I)

    # Remove single letters
    s = re.sub(r"\b\w\b", " ", s)

    # remove two letter consonant only words
    s = re.sub(r"\b[^aeiou\s]{2}\b", " ", s)

    # Remove specified substrings
    for substring in substrings_to_remove:
        s = s.replace(substring, " ")

    return s.strip()


def write_text(text, txt_file_path):
    os.makedirs(os.path.dirname(txt_file_path), exist_ok=True)
    # Write the extracted text to a TXT file
    with open(txt_file_path, "w", encoding="utf-8") as txt_file:
        txt_file.write(text)


if __name__ == "__main__":
    pre_raw = os.path.join("data", "pre-raw", "2_pali.zip")
    raw = os.path.join("data", "raw")
    unzip_file(pre_raw, raw)

    files = list_files_recursive(raw)
    curated = os.path.join("data", "curated")
    stop_words = stopwords.words("english") + [
        "th",
        "editions",
        "jaxxi",
        "jaxxii",
        "text",
        "society",
        "\x0e",
        "at",
        "edition",
        "pali",
        "printed",
        "see",
        "gretil",
        "sub",
        "uni",
        "goettingen",
        "file",
    ]

    stop_words = [f" {word} " for word in stop_words] + ["text", "society", "‘", "\x0e"]
    for file in tqdm(files):
        file_name = file.lstrip(str(raw)).rstrip(".htm")
        file_output = os.path.join(curated, file_name) + ".txt"
        text = html_to_text(file)
        text = remove_patterns(text, stop_words)
        if "text" in text:
            raise ValueError("text in text", file)
        write_text(text, file_output)
