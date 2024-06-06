import os  # for navigating downloaded files
import sys  # to get input arg
import urllib.request  # to download files
import zipfile  # to unzip downloaded files
import datetime  # to get hte current year

import datasets  # to make/upload a dataset
import pandas  # to handle the dataset
import lxml.etree  # For error handling
import tqdm  # for displaying progress

# Get API token from command line
HF_TOKEN = sys.argv[1]

# What years do we care about? Let's get all of them! We also need to download the 
# historical file though (see here: https://www.nsf.gov/awardsearch/download.jsp)
years = ["Historical"] + [
    str(y) for y in list(range(1959, datetime.datetime.now().year + 1))
]

# Step through each year, download the associated file, and parse it as set of xml files
for year in tqdm.tqdm(years, "Downloading annual reports"):

    # Download the file
    filename, _ = urllib.request.urlretrieve(
        "https://www.nsf.gov/awardsearch/download?DownloadFileName="
        + year
        + "&All=true"
    )

    # Unzip the file that we just downloaded
    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall(".")

# Make an empty list to add dicts too
awards = []

# For every xml file that we just exposed, turn it into a dict
for file in tqdm.tqdm(os.listdir("./"), "Loading awards"):
    if file.endswith(".xml"):
        try:
            awards.append(pandas.read_xml(file).to_dict(orient="records")[0])
            os.remove(file)
        except lxml.etree.XMLSyntaxError:
            print(file)

# Take the dicts, make a dataframe, make a dataset, and upload it
datasets.Dataset.from_pandas(pandas.DataFrame().from_dict(awards)).push_to_hub(
    "ccm/nsf-awards", token=HF_TOKEN
)
