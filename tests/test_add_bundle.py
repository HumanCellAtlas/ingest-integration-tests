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

        donors = primary_submission.retrieve_biomaterials_by_type('donor_organism')
        self.assertEqual(1, len(donors), msg='Expected exactly 1 Donor from primary submission.')

        project_uuid = projects[0].get_uuid()
        self.assertIsNotNone(project_uuid)

        Progress.report('Uploading addition spreadsheet...')

        # TODO create submission envelope that does NOT get exported; linking has to happen first!
        addition_submission = self._submit_dataset('additions', project_uuid=project_uuid)
        added_bundles = addition_submission.get_bundle_manifests()
        self.assertEqual(1, len(added_bundles), msg='Expected exactly 1 bundle to be added.')

        specimens = addition_submission.retrieve_biomaterials_by_type('specimen_from_organism')
        self.assertEqual(1, len(specimens), msg='Expected exactly 1 Specimen in the addition submission.')
        specimen = specimens[0]
        # do linking of process, specimen, donor from primary submission

    def _submit_dataset(self, dataset_name, project_uuid=None) -> IngestApiAgent.SubmissionEnvelope:
        dataset_fixture = DatasetFixture(dataset_name, config.deployment)
        self.runner.complete_run(dataset_fixture, project_uuid=project_uuid)
        return self.runner.submission_envelope
