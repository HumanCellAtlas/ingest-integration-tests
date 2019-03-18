import json
from os import listdir, path
from os.path import isfile, join


class AnalysisSubmissionFixture:
    def __init__(self):
        dir_path = path.dirname(path.realpath(__file__))
        self.analysis_process = self._load_file(f'{dir_path}/fixtures/analysis/10x/processes/analysis_process_0.json')
        self.analysis_protocol = self._load_file(f'{dir_path}/fixtures/analysis/10x/protocols/analysis_protocol_0.json')
        self.files = self._load_files(f'{dir_path}/fixtures/analysis/10x/files')

    def _load_file(self, location):

        with open(location, 'r') as f:
            obj = json.load(f)
        return obj

    def _load_files(self, dir):
        obj_list = []
        files = [f for f in listdir(dir) if isfile(join(dir, f))]
        for file in files:
            obj_list.append(self._load_file(join(dir, file)))
        return obj_list
