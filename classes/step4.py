import pandas as pd


def post_process(df: pd.DataFrame) -> pd.DataFrame:
    for c in ['saleprice', 'total2021', 'total2020', 'land2021', 'land2020', 'builtarea', 'landarea']:
        if df[c].dtype == float:
            continue
        df[c] = df[c].str.replace('$', '').str.replace(',', '').astype(float)

    for c in ['bedrooms', 'totalrooms', 'totalbaths', 'totalhalfbaths']:
        if df[c].dtype == float:
            continue
        df[c] = df[c].str.replace('Bedrooms', '') \
            .str.replace('Bedroom', '') \
            .str.replace('Rooms','') \
            .str.replace('Room','') \
            .str.replace('Bathrooms','') \
            .str.replace('Full','') \
            .str.replace('+','') \
            .str.strip().astype(float)

    for c in ['saledate']:
        df[c] = pd.to_datetime(df[c])

    return df
