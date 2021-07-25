import os
import re
import shutil
import requests
from zipfile import ZipFile
from hdx.hdx_configuration import Configuration
from hdx.data.organization import Organization
from tqdm import tqdm
from time import sleep
import pycountry

# this is your responsibility to update. it is the user agent that will be used by hdx
user_agent = "jacobhall"
# this should likely stay as-is. it indicates to hdx module what server to connect to
hdx_site = "prod"

##############################################################################


def unzip_list(zip_paths: list, dest_folder: str, rewrite_files=False):
    # we now have a list of zip file paths. let's unzip them
    # make sure destination folder is created
    unzip_dest = os.path.join(dest_folder, "unzipped")
    if not os.path.isdir(unzip_dest):
        try:
            os.mkdir(unzip_dest)
        except:
            raise Exception("Unable to create unzipped file destination folder!")
    print("Unzipping {} downloaded archives".format(len(zip_paths)))
    unzipped_paths = []
    for path in tqdm(zip_paths):
        if path in unzipped_paths:
            print("duplicate path detected and skipped: {}".format(path))
            continue
        elif not rewrite_files and os.path.exists(path):
            # TODO: check validity of existing unzipped file?
            continue
        try:
            ZipFile(path).extractall(unzip_dest)
            # os.remove(path)
            unzipped_paths.append(path)
        except:
            raise Exception("An error occured while extracting {}".format(path))
    return unzipped_paths


##############################################################################


def download_list(
    download_urls: list, # list of URLs to download
    dest_folder: str, # folder to save downloaded zips
    rewrite_files=False, # should files that already exist be rewritten? (default is skip)
    unzip_files=True, # should files be unzipped after downloading?
    courtesy_pause=0, # time in seconds we should wait between downloads
):
    # make sure dest_folder exists
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    print("Downloading {} zipped files".format(len(download_urls)))
    zip_paths = []
    for url in tqdm(download_urls):
        # some code from https://stackoverflow.com/q/56950987
        # and https://stackoverflow.com/a/56951135
        # determine name of file to write
        filename = url.split("/")[-1]
        # determine relative path of file to write
        file_path = os.path.join(dest_folder, filename)
        # if the file has already been written, perhaps we shouldn't write it again
        if not rewrite_files and os.path.exists(file_path):
            zip_paths.append(file_path)
            continue
        # TODO: make this respond to a changed dest_folder
        elif not rewrite_files and os.path.exists("zips/unzipped/" + filename):
            print("already-unzipped file found, skipping {}".format(filename))
            zip_paths.append(file_path)
            continue
        # attempt to make request
        request = requests.get(url, stream=True)
        # if request worked out, write file to path
        if request.ok:
            with open(file_path, "wb") as f:
                shutil.copyfileobj(request.raw, f)
                zip_paths.append(file_path)
        # request didn't work out. http fail code?
        else:
            print(
                "Download failed: status code {}\n{}".format(
                    request.status_code, request.text
                )
            )
        sleep(courtesy_pause)
    if unzip_files:
        return unzip_list(zip_paths, os.path.join(dest_folder, "unzipped"), rewrite_files=rewrite_files)
    else:
        return zip_paths


##############################################################################


def extract_country_code(name: str):
    code = None
    bits = re.split("_|\.|/", name)
    for bit in bits:
        country = pycountry.countries.get(alpha_3=bit)
        if country is not None:
            if code is None:
                code = country.alpha_3
            else:
                raise Exception(
                    "More than one valid country code found for name: {}".format(name)
                )
        else:
            country = pycountry.countries.get(name=bit)
            if country is not None:
                if code is None:
                    code = country.alpha_3
                else:
                    raise Exception(
                        "More than one valid country code found for name: {}".format(
                            name
                        )
                    )
    return code


##############################################################################

Configuration.create(hdx_site=hdx_site, user_agent=user_agent, hdx_read_only=True)

fb_org = Organization.read_from_hdx("facebook")

# get list of Facebook datasets matching query
datasets = fb_org.get_datasets("High Resolution Population Density Maps")

# get a list of download urls of the appropriate data files
download_urls = []
for dataset in datasets:
    if "highresolutionpopulationdensitymaps" in dataset["name"].replace("-", ""):
        for resource in dataset.get_resources():
            # if resource["format"] == "CSV" and not "general" in resource["url"]:
            if resource["format"] == "CSV" and (
                "population" in resource["url"] or "general" in resource["url"]
            ):
                download_urls.append(resource["download_url"])

# list created! now lets download each URL
if len(download_urls) > 0:
    paths = download_list(download_urls, "zips")
else:
    raise Exception("No matching resources found. Something is wrong!")
# unzip each path in zip_paths

for path in paths:
    print(path)
    print(extract_country_code(path))
