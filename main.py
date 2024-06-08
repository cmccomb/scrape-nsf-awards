import datetime  # to get hte current year
import io  # for reading downloaded files
import sys  # to get input arg
import urllib.request  # to download files
import zipfile  # to unzip downloaded files

import datasets  # to make/upload a dataset
import tqdm.auto  # for displaying progress
import xmltodict  # for converting xml

# Features that shouldn't be stringified during processing
float_features = {"AwardTotalIntnAmount", "AwardAmount"}

# Get API token from command line
HF_TOKEN: str = sys.argv[1]

# What years do we care about? Let's get all of them! We also need to download the
# historical file though (see here: https://www.nsf.gov/awardsearch/download.jsp)
years: list[str] = ["Historical"] + [
    str(y) for y in list(range(1959, datetime.datetime.now().year + 1))
]

# Make an empty list to add dicts too
awards: list[dict] = []

# Step through each year, download the associated file, and parse it as set of xml files
for year in tqdm.auto.tqdm(years, "Downloading and parsing by year... "):

    # Set up a request for the file
    req = urllib.request.Request(
        "https://www.nsf.gov/awardsearch/download?DownloadFileName="
        + year
        + "&All=true"
    )

    # Download the file
    with urllib.request.urlopen(req) as response:

        # Unzip the file that we just downloaded
        with zipfile.ZipFile(io.BytesIO(response.read())) as zip_file:

            # For every file in the zip, turn it into a dict
            for contained_file in zip_file.infolist():
                try:
                    with zip_file.open(contained_file.filename, "r") as f:
                        award = xmltodict.parse(f.read())["rootTag"]["Award"]
                        for k in list(set(award.keys()) - float_features):
                            award[k] = str(award[k])
                            if k in float_features:
                                award[k] = float(award[k])
                        awards.append(award)
                except xmltodict.expat.ExpatError as e:
                    pass

# Create a dataset from the list of dicts
print("creating dataset")
ds = datasets.Dataset.from_list(awards)

# Upload the dataset to huggingface
print("pushing to hub")
ds.push_to_hub(
    "ccm/nsf-awards", token=HF_TOKEN
)
