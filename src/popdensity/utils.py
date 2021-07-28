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

##############################################################################


def unzip_list(zip_paths: list, dest_folder: str, rewrite_files=False, cleanup=False):
    # we now have a list of zip file paths. let's unzip them
    # make sure destination folder is created
    if not os.path.isdir(dest_folder):
        try:
            os.mkdir(dest_folder)
        except:
            raise Exception("Unable to create unzipped file destination folder!")
    print("Unzipping {} downloaded archives".format(len(zip_paths)))
    unzipped_paths = []
    for path in tqdm(zip_paths):
        if path in unzipped_paths:
            print("duplicate path detected and skipped: {}".format(path))
            continue
        # TODO: check if file has already been unzipped
        """
        elif not rewrite_files and os.path.exists(path):
            # TODO: check validity of existing unzipped file?
            continue
        """
        try:
            ZipFile(path).extractall(dest_folder)
            if cleanup:
                os.remove(path)
            unzipped_paths.append(path)
        except:
            raise Exception("An error occured while extracting {}".format(path))
    return unzipped_paths


##############################################################################


def download_list(
    download_urls: list,  # list of URLs to download
    dest_folder: str,  # folder to save downloaded zips
    rewrite_files=False,  # should files that already exist be rewritten? (default is skip)
    unzip_files=True,  # should files be unzipped after downloading?
    courtesy_pause=0,  # time in seconds we should wait between downloads
):
    # make sure dest_folder exists
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    print("Downloading {} zipped files".format(len(download_urls)))
    zip_paths = []
    already_unzipped_count = 0
    for url in tqdm(download_urls):
        # some code from https://stackoverflow.com/q/56950987
        # and https://stackoverflow.com/a/56951135
        # determine name of file to write
        filename = url.split("/")[-1]
        # determine relative path of file to write
        file_path = os.path.join(dest_folder, filename)

        """
        # determine name of destination unzipped file
        if filename.endswith(".zip"):
            unzip_dest = os.path.join(dest_folder, "unzipped", filename[:-4])
            print(unzip_dest)
        # else: warn("non-zip file downloaded")
    
        # if an unzipped version of this file already exists, perhaps we shouldn't
        # bother downloading it
        if os.path.exists(unzip_dest) and not rewrite_files:
            print("true when it shouldnt be")
            zip_paths.append(file_path)
            already_unzipped_count += 1
            continue
        ...elif
        """
        # if the file has already been written, perhaps we shouldn't write it again
        if not rewrite_files and os.path.exists(file_path):
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
    if already_unzipped_count > 0:
        print(
            "Skipped {} files that have already been downloaded and unzipped".format(
                already_unzipped_count
            )
        )
    if unzip_files and already_unzipped_count != len(download_urls):
        return unzip_list(
            zip_paths,
            os.path.join(dest_folder, "unzipped"),
            rewrite_files=rewrite_files,
        )
    else:
        return zip_paths


##############################################################################


def extract_country_code(name: str):
    code = None
    bits = re.split("_|\.|/|-", name)
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
