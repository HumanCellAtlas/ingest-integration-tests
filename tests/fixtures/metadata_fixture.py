from os import path

from tests.fixtures.util import load_file


class MetadataFixture:
    def __init__(self):
        dir_path = path.dirname(path.realpath(__file__))
        self.biomaterial = load_file(f'{dir_path}/metadata/donor_organism.json')
        self.sequence_file = load_file(
            f'{dir_path}/metadata/sequence_file.json')

        self.data_files_location = 's3://org-humancellatlas-dcp-test-data/10x/'
