import os
import zipfile

import requests


def list_files_recursive(directory):
    all_files = []
    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            all_files.append(os.path.join(dirpath, filename))
    return all_files


def unzip_file(zip_filepath, dest_dir):
    """The function `unzip_file` extracts all files from a given zip file and saves them to a specified destination directory.

    :param zip_filepath: The path to the zip file that you want to unzip
    :param dest_dir: The destination directory where the files from the zip file will be extracted to
    """
    with zipfile.ZipFile(zip_filepath, "r") as zip_ref:
        zip_ref.extractall(dest_dir)


def write_text(text, txt_file_path):
    os.makedirs(os.path.dirname(txt_file_path), exist_ok=True)
    # Write the extracted text to a TXT file
    with open(txt_file_path, "w", encoding="utf-8") as txt_file:
        txt_file.write(text)


def get_http(url: str):
    response = requests.get(url, allow_redirects=True)

    response.raise_for_status()
    return response.content


def decode_string(s):
    return s.encode("latin1").decode("unicode_escape")
