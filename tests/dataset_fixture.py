import glob
import json
import os

import requests


class DatasetFixture:

    """
    Local test fixture datasets must be laid out as follows:
         dataset-folder/
            <some-spreadsheet>.xlsx
            data-files/
                <data_file_1>
                <data_file_2>
                ...

    """

    def __init__(self, dataset_name, deployment="integration"):
        self.name = dataset_name
        self.deployment = deployment

        self.config = {}
        self.dataset_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), 'fixtures/datasets',
                         self.name))
        self._spreadsheet = None
        readme_json_path = os.path.join(self.dataset_path, 'README.json')

        with open(readme_json_path) as json_data:
            self.config = json.load(json_data)
            self.config["spreadsheet_location"] = self.config[
                "spreadsheet_location"].replace("DEPLOYMENT", self.deployment)
        self._download_spreadsheet()

    def _download_spreadsheet(self):
        response = requests.get(self.config["spreadsheet_location"])
        with open(self.metadata_spreadsheet_path, 'wb') as f:
            f.write(response.content)

    @property
    def metadata_spreadsheet_path(self):
        filename = self.name + '.xlsx'
        return os.path.join(self.dataset_path, filename)

