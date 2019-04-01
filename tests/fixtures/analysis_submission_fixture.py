from os import path

from tests.fixtures.util import load_file, load_files


class AnalysisSubmissionFixture:
    def __init__(self):
        dir_path = path.dirname(path.realpath(__file__))
        self.analysis_process = load_file(f'{dir_path}/analyses/10x/processes/analysis_process_0.json')
        self.analysis_protocol = load_file(f'{dir_path}/analyses/10x/protocols/analysis_protocol_0.json')
        self.files = load_files(f'{dir_path}/analyses/10x/files')
