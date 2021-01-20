import pickle

import pandas as pd

from classes.vsgi import VGSI

if __name__ == "__main__":
    town = 'lexingtonma'
    v = VGSI(town)

    urls = v.step1_scrape_links()
    with open(f'{town}-1-urls.p', 'wb') as f:
        pickle.dump(urls, f)

    urls = None
    with open(f'{town}-1-urls.p', 'rb') as f:
        urls = pickle.load(f)
    v.step2_download_htmls([url.strip() for url in urls])

    df = v.step3_parse_htmls()
    df.to_pickle(f'{town}-3-raw.p')

    df = pd.read_pickle(f'{town}-3-raw.p')
    df = v.step4_post_process(df)

    df.to_pickle(f'{town}-4.p')

    # further processing
    sf = df[df['usecodedesc'] == 'Single Fam  MDL-01']  # single family homes
    sf[['builtarea', 'landarea']].describe()  # areas
    sf[['total2021', 'total2020']].describe()  # values

    print()

# df.sample().iloc[0]
# pid                              4996
# location            376 CATERINA HTS
# owner                   SKOLNIK IRA L
# coowner               SKOLNIK LINDA R
# saleprice                   1.206e+06
# saledate          2010-11-15 00:00:00
# yearbuilt                        1987
# builtarea                        4724
# usecode                          1010
# usecodedesc        Single Fam  MDL-01
# zone                               AA
# neighborhood                       29
# landarea                         1.94
# style                  Contemp/Modern
# model                     Residential
# bedrooms                            4
# totalbaths                          3
# totalhalfbaths                      1
# totalrooms                          9
# total2021                         NaN
# total2020                  1.4912e+06
# land2021                          NaN
# land2020                       348800
