import multiprocessing as mp

import requests
from tqdm import tqdm


def download_one_html(data):
    url, filename = data

    try:
        with open(filename, 'w') as file:
            file.write(requests.get(url).text)
    except:
        pass


def download(folder: str, urls: list):
    urls = [(url, f'{folder}{hash(url)}.html') for url in urls]

    with mp.Pool() as pool:
        for _ in tqdm(
            pool.imap_unordered(download_one_html, urls),
            desc='Downloading addresses',
            total=len(urls),
        ):
            pass
