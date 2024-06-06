import os  # for navigating downloaded files
import sys  # to get input arg
import urllib.request  # to download files
import zipfile  # to unzip downloaded files
import datetime  # to get hte current year

import datasets  # to make/upload a dataset
import pandas  # to handle the dataset

# Get API token from command line
HF_TOKEN = sys.argv[1]

# Make an empty list to add dicts too
dicts = []

# What years do we care about?
years = ["Historical"] + [
    str(y) for y in list(range(1959, datetime.datetime.now().year + 1))
]

for year in years:
    print(year)
    filename, headers = urllib.request.urlretrieve(
        "https://www.nsf.gov/awardsearch/download?DownloadFileName="
        + year
        + "&All=true"
    )
    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall(".")
        for file in os.listdir("./"):
            if file.endswith(".xml"):
                dicts.append(pandas.read_xml(file).to_dict(orient="records")[0])
                os.remove(file)

datasets.Dataset.from_pandas(pandas.DataFrame().from_dict(dicts)).push_to_hub(
    "ccm/nsf-awards", token=HF_TOKEN
)
