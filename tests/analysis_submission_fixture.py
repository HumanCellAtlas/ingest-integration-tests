import json
from os import listdir
from os.path import isfile, join


class AnalysisSubmissionFixture:
    def __init_(self):
        self.analysis_process = self._load_file('processes/analysis_process.json')
        self.analysis_protocol = self._load_file('protocols/analysis_protocol.json')
        self.files = self._load_files('files')
        self.data_files = self._load_files('data')

    def _load_file(self, location):
        with open(location, 'r') as f:
            obj = json.load(f)
        return obj

    def _load_files(self, dir):
        obj_list = []
        files = [f for f in listdir(dir) if isfile(join(dir, f))]
        for file in files:
            obj_list.append(self._load_file(file))
        return  obj_list
