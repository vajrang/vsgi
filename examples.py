import pandas as pd

from classes.vsgi import VGSI

if __name__ == "__main__":
    town = 'concordma'
    v = VGSI(town)

    urls = v.step1_scrape_links()
    with open('urls.txt', 'w') as f:
        f.writelines([url + '\n' for url in urls])

    urls = None
    with open('urls.txt', 'r') as f:
        urls = f.readlines()
    v.step2_download_htmls([url.strip() for url in urls])

    df = v.step3_parse_htmls()
    df.to_pickle(f'{town}.p')

    df = pd.read_pickle(f'{town}.p')
    df = v.step4_post_process(df)

    print()

# df.sample().iloc[0]
# pid                             1494
# location                   81 OAK ST
# owner             ANDERSON JAMES J &
# coowner            ANDERSON JANICE F
# saleprice                         $1
# saledate                  03/19/2009
# yearbuilt                       1972
# builtarea                      1,819
# usecode                         1010
# usecodedesc       Single Fam  MDL-01
# zone                              RS
# neighborhood                      20
# landarea                        5400
# style                  TRAD/GAR COL.
# model                    Residential
# bedrooms                  3 Bedrooms
# totalbaths                         1
# totalhalfbaths                     1
# totalrooms                         7
# total2021                   $808,000
# total2020                   $800,000
# land2021                    $448,000
# land2020                    $448,000
# Name: 1071, dtype: object
