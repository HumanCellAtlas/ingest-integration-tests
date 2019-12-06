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
        primary_submission = self._submit_dataset('SS2')
        projects = primary_submission.retrieve_projects()
        self.assertEqual(1, len(projects))

        biomaterials = primary_submission.retrieve_biomaterials()
        donors = [entity for entity in biomaterials if entity.get_concrete_type().lower() == 'donor_organism']
        self.assertEqual(1, len(donors), msg='Expected exactly 1 Donor from primary submission.')

        project_uuid = projects[0].get_uuid()
        self.assertIsNotNone(project_uuid)

        Progress.report('Uploading addition spreadsheet...')
        addition_submission = self._submit_dataset('additions', project_uuid=project_uuid)
        added_bundles = addition_submission.get_bundle_manifests()
        self.assertEqual(1, len(added_bundles), msg='Expected exactly 1 bundle to be added.')

    def _submit_dataset(self, dataset_name, project_uuid=None) -> IngestApiAgent.SubmissionEnvelope:
        dataset_fixture = DatasetFixture(dataset_name, config.deployment)
        self.runner.complete_run(dataset_fixture, project_uuid=project_uuid)
        return self.runner.submission_envelope
