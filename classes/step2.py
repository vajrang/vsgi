import os
import time
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from urllib.parse import parse_qs, urlparse

import requests
from tqdm import tqdm

from . import utils


def download_one_html(data):
    url, filename = data

    # if os.path.isfile(filename) and os.stat(filename).st_size > 0:
    #     return

    try:
        with open(filename, 'w') as file:
            file.write(requests.get(url).text)
    except:
        pass


def download_chunk(data: list):
    with ThreadPool() as pool:
        for _ in pool.imap_unordered(download_one_html, data):
            pass


def get_filename(url):
    r = parse_qs(urlparse(url).query).get('pid')
    assert len(r) == 1
    return r[0]


def download(folder: str, urls: list):
    urls = [(url, f'{folder}{get_filename(url)}.html') for url in urls]

    with Pool() as pool:
        print(f'Downloading in {pool._processes} processes')
        chunks = utils.chunk(urls, pool._processes)

        t0 = time.perf_counter()
        for _ in pool.imap_unordered(download_chunk, chunks):
            pass
        t1 = time.perf_counter()

        print(f'Done, took {t1-t0:.2f} secs')
