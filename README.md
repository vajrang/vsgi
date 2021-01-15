# VSGI (Vision Goverment Solutions) Scraper

Utility to scrape property data from https://gis.vgsi.com/

Each step can be quite time consuming, so they are separated by design so that you may pickle/save the results from each step and proceed later.

All underlying functions map-reduce the problem by using python's ```multiprocessing``` module. 

This has been tested to work with ```'lexingtonma'``` and ```'concordma'```.

Please see ```example.py``` for more code samples.

### Usage:

```python
from vsgi import VSGI

v = VSGI('lexingtonma')
# Step 1: scrape all links for each property, returned as list of URLs
urls = v.step1_scrape_links()

# Step 2: download each page as html and store locally
v.step2_download_htmls(urls)

# Step 3: parse all htmls and combine into pandas DataFrame
df = v.step3_parse_htmls()

# saving the DataFrame highly recommended
df.to_pickle('lexingtonma.p')

# Step 4: post-process the DataFrame
df = v.step4_post_process(df)

# continue with further processing of the DataFrame
```
