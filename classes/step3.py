import os
import pickle
import time
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

from . import utils


def get_text_from_id(soup, id):
    assert soup
    assert id
    tags = soup.find_all(id=id)
    if len(tags) == 0:
        return None
    elif len(tags) == 1:
        return tags[0].get_text()
    assert False


def get_value(df, field_list):
    for f in field_list:
        v = df.get(f)
        if v:
            return v
    # assert False
    return None


def parse_html_content(html_content: str):
    soup = BeautifulSoup(html_content, 'lxml')

    retval = {}
    retval['pid'] = get_text_from_id(soup, 'MainContent_lblPid')
    retval['location'] = get_text_from_id(soup, 'MainContent_lblLocation')
    retval['owner'] = get_text_from_id(soup, 'MainContent_lblOwner')
    retval['coowner'] = get_text_from_id(soup, 'MainContent_lblCoOwner')
    retval['saleprice'] = get_text_from_id(soup, 'MainContent_lblPrice')
    retval['saledate'] = get_text_from_id(soup, 'MainContent_lblSaleDate')
    retval['yearbuilt'] = get_text_from_id(soup, 'MainContent_ctl01_lblYearBuilt')
    retval['builtarea'] = get_text_from_id(soup, 'MainContent_ctl01_lblBldArea')
    retval['usecode'] = get_text_from_id(soup, 'MainContent_lblUseCode')
    retval['usecodedesc'] = get_text_from_id(soup, 'MainContent_lblUseCodeDescription')
    retval['zone'] = get_text_from_id(soup, 'MainContent_lblZone')
    retval['neighborhood'] = get_text_from_id(soup, 'MainContent_lblNbhd')
    retval['landarea'] = get_text_from_id(soup, 'MainContent_lblLndSf')\
                        or get_text_from_id(soup, 'MainContent_lblLndAcres')
    retval['builtarea'] = get_text_from_id(soup, 'MainContent_ctl01_lblBldArea')

    try:
        bldg_df = pd.read_html(html_content, match='Building Attributes')[0]
        if not bldg_df.empty and 'Field' in bldg_df.columns:
            bldg_df = bldg_df.set_index('Field')
        retval['style'] = get_value(bldg_df['Description'], ['Style:', 'Style'])
        retval['model'] = get_value(bldg_df['Description'], ['Model:', 'Model'])
        retval['bedrooms'] = get_value(bldg_df['Description'], ['Total Bedrooms:', 'Ttl Bedrms:', 'Total Bedrms'])
        retval['totalbaths'] = get_value(bldg_df['Description'], ['Total Bthrms:', 'Ttl Bathrms:', 'Total Baths'])
        retval['totalhalfbaths'] = get_value(bldg_df['Description'], ['Total Half Baths:', 'Ttl Half Bths:'])
        retval['totalrooms'] = get_value(bldg_df['Description'], ['Total Rooms:', 'Total Rooms'])
    except:
        pass

    try:
        val_df = pd.read_html(html_content, match='Assessment')[-1]
        if not val_df.empty and 'Valuation Year' in val_df.columns:
            val_df = val_df.set_index('Valuation Year')
        retval['total2021'] = val_df['Total'].get(2021)
        retval['total2020'] = val_df['Total'].get(2020)
        retval['land2021'] = val_df['Land'].get(2021)
        retval['land2020'] = val_df['Land'].get(2020)
    except:
        pass

    return retval


def process_one_html(filename):
    if not os.path.exists(filename):
        return None
    with open(filename, 'r') as file:
        try:
            html_content = file.read()
            return parse_html_content(html_content)
        except:
            print(f'{filename} had a problem')
    return None


def process_chunk(data):
    results = []
    with ThreadPool() as pool:
        for result in pool.imap_unordered(process_one_html, data):
            if result:
                results.append(result)
    return results


def parse_all_htmls(folder: str) -> pd.DataFrame:
    htmls = [os.path.join(folder, file) for file in os.listdir(folder)]

    results = []
    with Pool() as pool:
        print(f'Processing in {pool._processes} processes')
        chunks = utils.chunk(htmls, pool._processes)

        t0 = time.perf_counter()
        for result in pool.imap_unordered(process_chunk, chunks):
            results.extend(result)
        t1 = time.perf_counter()

        print(f'Done, took {t1-t0:.2f} secs')

    return pd.DataFrame(results)
