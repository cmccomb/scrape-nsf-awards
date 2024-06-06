import urllib.request
import zipfile
import pandas
import datasets
import os
import sys


# Get API token from command line
HF_TOKEN = sys.argv[1]

dicts = []

for year in ["Historical"] + [str(y) for y in list(range(1959, 2024))]:
    print(year)
    filename, headers = urllib.request.urlretrieve("https://www.nsf.gov/awardsearch/download?DownloadFileName=" + year + "&All=true")
    with zipfile.ZipFile(filename, 'r') as zip_ref:
        zip_ref.extractall(".")
        for file in os.listdir("./"):
            if file.endswith(".xml"):
                try:
                    dicts.append(pandas.read_xml(file).to_dict(orient="records")[0])
                except:
                    print("\t" + file)
                os.remove(file)

dataset = datasets.Dataset.from_pandas(
    pandas.DataFrame().from_dict(dicts)
)

dataset.push_to_hub("ccm/nsf-awards", token=HF_TOKEN)
