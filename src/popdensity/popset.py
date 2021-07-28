from hdx.hdx_configuration import Configuration
from hdx.data.organization import Organization
import pycountry
from .utils import download_list, extract_country_code


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
        return paths

    def deDup(self):
        return

    def getCSV(self, file_path):
        return

    def getRaster(self, file_path):
        return
