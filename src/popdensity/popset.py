from hdx.hdx_configuration import Configuration
from hdx.data.organization import Organization
import pycountry
from .utils import download_list, extract_country_code, check_df
import pandas as pd


class PopSet:
    def __init__(self, query, user_agent):
        Configuration.create(hdx_site="prod", user_agent=user_agent, hdx_read_only=True)

        self.query = []

        # check if query is valid
        if query == "global":
            self.query = "global"
        elif isinstance(query, str):
            query = [query]
        elif not isinstance(query, list):
            raise TypeError("query not string or list of strings")
        for q in query:
            country = pycountry.countries.get(alpha_3=q)
            if country is None:
                raise ValueError(
                    "one of the passed queries was not recognized: {}".format(q)
                )
            else:
                self.query.append(country.alpha_3)

    def sendQuery(self):
        self.download_urls = []

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
                        for q in self.query:
                            if (
                                q == "global"
                                or extract_country_code(dataset["name"]) == q
                                or (
                                    len(dataset["solr_additions"]) == 0
                                    and extract_country_code(
                                        dataset["solr_additions"][0]
                                    )
                                    == q
                                )
                            ):
                                self.download_urls.append(resource["download_url"])

        if len(self.download_urls) == 0:
            raise Exception("could not find your requested country!")

    def retrieveData(self, unzip=True):
        if len(self.download_urls) > 0:
            paths = download_list(self.download_urls, "zips", unzip_files=unzip)
        else:
            raise Exception("No matching resources found. Something is wrong!")
        self.unzipped_paths = paths
        return paths

    def deDup(self):
        return

    def getCSV(self, file_path):
        # TODO: check if self.unzipped_paths exists and is non-empty
        # TODO: check if output filename does not already exist
        for filename in self.unzipped_paths:
            if filename == self.unzipped_paths[0]:
                first_file = True
                print("this should appear once, before first processing")
            else:
                first_file = False
            print("processing " + filename)
            thisdf = pd.read_csv(filename)
            thisdf = check_df(thisdf)
            print("made it this far")
            thisdf.to_csv(file_path, mode="a", header=first_file)
            del thisdf
        return

    def getRaster(self, file_path):
        return
