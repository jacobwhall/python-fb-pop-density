import os
from hdx.hdx_configuration import Configuration
from hdx.data.organization import Organization
from time import sleep
import pycountry
from .utils import download_list, extract_country_code


class popset:

    download_urls = []
    datasets = None

    def __init__(self, query, user_agent):
        Configuration.create(hdx_site="prod", user_agent=user_agent, hdx_read_only=True)

        # check if query is valid
        if query != "global":
            country = pycountry.countries.get(alpha_3=query)
            if country is None:
                raise ValueError('Query not recognized as valid country or "global"')
            else:
                query = country.alpha_3

        fb_org = Organization.read_from_hdx("facebook")
        # get list of Facebook population density datasets
        self.datasets = fb_org.get_datasets("High Resolution Population Density Maps")

        # get a list of download urls of the appropriate data files
        for dataset in self.datasets:
            if "highresolutionpopulationdensitymaps" in dataset["name"].replace(
                "-", ""
            ):
                for resource in dataset.get_resources():
                    # if resource["format"] == "CSV" and not "general" in resource["url"]:
                    if resource["format"] == "CSV" and (
                        "population" in resource["url"] or "general" in resource["url"]
                    ):
                        if (
                            query == "global"
                            or extract_country_code(dataset["name"]) == query
                            or (
                                len(dataset["solr_additions"]) == 0
                                and extract_country_code(dataset["solr_additions"][0])
                                == query
                            )
                        ):
                            self.download_urls.append(resource["download_url"])

        if len(self.download_urls) == 0:
            raise Exception("could not find your requested country!")

    def retrieveData(self, unzip=True):
        if len(self.download_urls) > 0:
            paths = download_list(self.download_urls, "bips", unzip_files=unzip)
        else:
            raise Exception("No matching resources found. Something is wrong!")
        return paths

    def getCSV(self, file_path):
        return

    def getRaster(self, file_path):
        return
