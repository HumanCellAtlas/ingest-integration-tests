from unittest import TestCase

from tests import config
from tests.fixtures.dataset_fixture import DatasetFixture
from tests.ingest_agents import IngestApiAgent
from tests.runners.dataset_runner import DatasetRunner
from tests.utils import Progress


class AddBundleTest(TestCase):

    def setUp(self) -> None:
        self.runner = DatasetRunner(config.deployment)

    def test_run(self) -> None:
        submission_envelope = self._submit_dataset('SS2')
        projects = submission_envelope.retrieve_projects()
        self.assertEqual(1, len(projects))

        project_uuid = projects[0].get_uuid()
        self.assertIsNotNone(project_uuid)

        Progress.report('Uploading addition spreadsheet...')
        self._submit_dataset('additions', project_uuid=project_uuid)

    def _submit_dataset(self, dataset_name, project_uuid=None) -> IngestApiAgent.SubmissionEnvelope:
        dataset_fixture = DatasetFixture(dataset_name, config.deployment)
        self.runner.complete_run(dataset_fixture, project_uuid=project_uuid)
        return self.runner.submission_envelope
