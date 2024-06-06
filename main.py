import os
import sys
import urllib.request
import zipfile

import datasets
import pandas

# Get API token from command line
HF_TOKEN = sys.argv[1]

dicts = []

for year in ["Historical"] + [str(y) for y in list(range(1959, 1962))]:
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
                try:
                    dicts.append(pandas.read_xml(file).to_dict(orient="records")[0])
                except:
                    print("\t" + file)
                os.remove(file)

datasets.Dataset.from_pandas(pandas.DataFrame().from_dict(dicts)).push_to_hub(
    "ccm/nsf-awards", token=HF_TOKEN
)
