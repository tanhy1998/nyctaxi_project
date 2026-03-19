import urllib.request
import os
import shutil

def download_file(url: str, dir_path: str, local_path: str):
    """
    Downloads a file from the given URL to the local_path.
    """
    os.makedirs(os.path.dirname(dir_path), exist_ok=True)

    with urllib.request.urlopen(url) as response, open(local_pathm "wb") as out_files:
        shutil.copyfileobj(response, out_file) 