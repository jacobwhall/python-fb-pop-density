import os
import re
import shutil
import requests
from zipfile import ZipFile, ZipInfo
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
            for info in ZipFile(path).infolist():
                if "csv" in info.filename:
                    unzipped_paths.append(os.path.join(dest_folder, info.filename))
                    csv_detected = True
                    break
            """
            if not csv_detected:
                warn("CSV file not detected while extracting {}".format(path))
            """
            ZipFile(path).extractall(dest_folder)
            if cleanup:
                os.remove(path)
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
    already_downloaded_count = 0
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
            already_downloaded_count += 1
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
    if already_downloaded_count > 0:
        print(
            "Skipped downloading {} files that already exist".format(
                already_downloaded_count
            )
        )
    if len(zip_paths) > 0:
        return unzip_list(
            zip_paths,
            os.path.join(dest_folder, "unzipped"),
            rewrite_files=rewrite_files,
        )
    else:
        return None


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


##############################################################################


def check_df(dataframe):
    # TODO: handle more than three columns
    if len(dataframe.columns) > 3:
        raise NotImplementedError(
            "More than one population density column detected. Support for this has not been added."
        )

    # if first is lat, second is long
    if dataframe.columns[0].lower() in ["lat", "latitude"] and dataframe.columns[1].lower() in ["long", "lon", "longitude"]:
        return dataframe.set_axis(["lat", "long", "pop"], axis=1, inplace=False)
    # elif first is long, second is lat
    elif dataframe.columns[0].lower() in ["long", "lon", "longitude"] and dataframe.columns[1].lower() in ["lat", "latitude"]:
        cols = dataframe.columns
        # switch first two columns so that output order is lat, long
        return dataframe[[cols[1], cols[0], cols[2]]].set_axis(["lat", "long", "pop"], axis=1, inplace=False)
    else:
        raise ValueError("First two columns not detected as latitude/longitude")

    return dataframe.set_axis(["long", "lat", "pop"], axis=1, inplace=False)
