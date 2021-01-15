from bs4 import BeautifulSoup
import requests
import regex as re
import multiprocessing as mp
from tqdm import tqdm


def get_links_matching_pattern(url, pattern):
    html_content = requests.get(url).text

    soup = BeautifulSoup(html_content, 'lxml')
    links = soup.find_all('a', href=True)

    hrefs = [l['href'] for l in links]
    regpat = re.compile(pattern)
    links = [href for href in hrefs if regpat.match(href)]

    return links


def get_letter_urls(url):
    pstr = r'Streets\.aspx\?Letter=(\w)'
    return get_links_matching_pattern(url, pstr)


def get_streets(url):
    assert url is not None
    pstr = r'Streets\.aspx\?Name=.+'
    return get_links_matching_pattern(url, pstr)


def get_addresses(url):
    assert url is not None
    pstr = r'Parcel\.aspx\?pid=\d+'
    return get_links_matching_pattern(url, pstr)


def scrape(urlroot, startfrom):
    letter_urls = get_letter_urls(urlroot + startfrom)
    letter_urls = [urlroot + l for l in letter_urls]

    address_urls = []
    with mp.Pool() as pool:
        street_urls = []
        for result in tqdm(
            pool.imap_unordered(get_streets, letter_urls),
            desc='Get streets',
            total=len(letter_urls),
        ):
            street_urls.extend(result)
        street_urls = [urlroot + l for l in street_urls]

        for result in tqdm(
            pool.imap_unordered(get_addresses, street_urls),
            desc='Get addresses',
            total=len(street_urls),
        ):
            address_urls.extend(result)
        address_urls = [urlroot + l for l in address_urls]

    return address_urls


if __name__ == '__main__':
    address_urls = scrape('https://gis.vgsi.com/lexingtonma/', 'Streets.aspx')

    with open('data/lex-address-urls.txt', 'w') as f:
        f.writelines(f'{address}\n' for address in address_urls)

    print()
