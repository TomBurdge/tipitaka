import re
import string
from collections import Counter

from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize


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
