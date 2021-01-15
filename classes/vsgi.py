import multiprocessing as mp
import os

import pandas as pd

from . import step1, step2, step3, step4


class VGSI():
    """
    Scraper for assessors data from gis.vgsi.com, given a town name
    
    e.g. Lexington, MA is available at https://gis.vgsi.com/lexingtonma/
    """

    GIS_URL_ROOT = 'https://gis.vgsi.com/'
    GIS_ORIGIN_PAGE = 'Streets.aspx'
    DATA_FOLDER_ROOT = 'data/'

    def __init__(self, town: str, datafolder: str = None) -> None:
        """Create VSGI instance
        Able to perform the following steps. Separated to be able to run independantly.
            1. Recursively scrape all links from street listing
            2. Download all html pages for each listing
            3. Parse all html files for data and put into pandas DataFrame
            4. Post-process the DataFrame to normalize data

        Args:
            town (str): name of town, e.g. lexingtonma or concordma
            datafolder (str, optional): local folder to hold scraped html files. e.g. data/lexingtonma/
                                        if none provided, will create a folder called data/{town}/
        """
        super().__init__()
        if not datafolder:
            datafolder = f'{self.DATA_FOLDER_ROOT}{town}/'

        self.town = town
        self.datafolder = datafolder

        os.makedirs(self.datafolder, exist_ok=True)

    @property
    def town_root_url(self):
        return f'{self.GIS_URL_ROOT}{self.town}/'

    def step1_scrape_links(self) -> list:
        return step1.scrape(self.town_root_url, self.GIS_ORIGIN_PAGE)

    def step2_download_htmls(self, urls: list):
        return step2.download(self.datafolder, urls)

    def step3_parse_htmls(self) -> pd.DataFrame:
        return step3.parse_all_htmls(self.datafolder)

    def step4_post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        return step4.post_process(df)
