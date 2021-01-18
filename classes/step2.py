import time
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

import requests
from tqdm import tqdm

from . import utils


def download_one_html(data):
    url, filename = data

    try:
        with open(filename, 'w') as file:
            file.write(requests.get(url).text)
    except:
        pass


def download_chunk(data: list):
    with ThreadPool() as pool:
        for _ in pool.imap_unordered(download_one_html, data):
            pass


def download(folder: str, urls: list):
    urls = [(url, f'{folder}{hash(url)}.html') for url in urls]

    with Pool() as pool:
        print(f'Downloading in {pool._processes} processes')
        chunks = utils.chunk(urls, pool._processes)

        t0 = time.perf_counter()
        for _ in pool.imap_unordered(download_chunk, chunks):
            pass
        t1 = time.perf_counter()

        print(f'Done, took {t1-t0} secs')
