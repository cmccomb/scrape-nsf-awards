import datetime  # to get hte current year
import io  # for reading downloaded files
import sys  # to get input arg
import urllib.request  # to download files
import zipfile  # to unzip downloaded files

import datasets  # to make/upload a dataset
import tqdm.auto  # for displaying progress
import xmltodict  # for converting xml

# Get API token from command line
HUGGINGFACE_TOKEN: str = sys.argv[1]

# Features that should be floated during processing
float_features: set[str] = {"AwardTotalIntnAmount", "AwardAmount"}

# What years do we care about? Let's get all of them! (see here: https://www.nsf.gov/awardsearch/download.jsp)
years_of_interest: list[str] = [
    str(y) for y in list(range(1959, datetime.datetime.now().year + 1))
]

# Make an empty list to add dicts too
awards: list[dict] = []

# Step through each year, download the associated file, and parse it as set of xml files
for year in tqdm.auto.tqdm(years_of_interest, "Downloading and parsing by year... "):

    # Set up a request for the file
    request = urllib.request.Request(
        "https://www.nsf.gov/awardsearch/download?DownloadFileName="
        + year
        + "&All=true"
    )

    # Download the file
    with urllib.request.urlopen(request) as response:

        # Unzip the file that we just downloaded
        with zipfile.ZipFile(io.BytesIO(response.read())) as zip_file:

            # For every file in the zip, turn it into a dict
            for contained_file in zip_file.infolist():
                try:
                    with zip_file.open(contained_file.filename, "r") as file:
                        award: dict = xmltodict.parse(file.read())["rootTag"]["Award"]
                        for key in award.keys():
                            if key in float_features:
                                award[key] = float(award[key] or "nan")
                            else:
                                award[key] = str(award[key])
                        awards.append(award)

                # Sometimes the file is not well-formed, so an error is thrown.
                # We just chalk this up as a loss.
                except xmltodict.expat.ExpatError:
                    pass

# Create a dataset from the list of dicts
dataset = datasets.Dataset.from_list(awards)

# Upload the dataset to huggingface
dataset.push_to_hub("ccm/nsf-awards", token=HUGGINGFACE_TOKEN)
