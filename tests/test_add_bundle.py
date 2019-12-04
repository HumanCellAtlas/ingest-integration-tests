from unittest import TestCase

from tests import config
from tests.fixtures.dataset_fixture import DatasetFixture
from tests.runners.dataset_runner import DatasetRunner


class AddBundleTest(TestCase):

    def run(self, *args):
        runner = DatasetRunner(config.deployment)
        dataset_fixture = DatasetFixture('SS2', config.deployment)
        runner.complete_run(dataset_fixture)
