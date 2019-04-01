from os import path

from tests.fixtures.util import load_file


class MetadataFixture:
    def __init__(self):
        dir_path = path.dirname(path.realpath(__file__))
        self.biomaterial = load_file(f'{dir_path}/metadata/donor_organism.json')
