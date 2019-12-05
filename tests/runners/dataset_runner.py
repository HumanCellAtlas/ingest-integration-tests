import os

from tests.ingest_agents import IngestUIAgent, IngestApiAgent
from tests.runners.submission_manager import SubmissionManager
from tests.utils import Progress


class DatasetRunner:

    def __init__(self, deployment):
        self.deployment = deployment

        self.ingest_broker = IngestUIAgent(deployment)
        self.ingest_api = IngestApiAgent(deployment=deployment)
        self.submission_id = None
        self.submission_envelope = None

        self.dataset = None

        self.submission_manager = None

    def valid_run(self, dataset_fixture):
        self.dataset = dataset_fixture
        self.upload_spreadsheet_and_create_submission(dataset_fixture)
        self.submission_manager = SubmissionManager(self.submission_envelope)
        self.submission_manager.get_upload_area_credentials()
        self.submission_manager.stage_data_files(self.dataset.config['data_files_location'])
        self.submission_manager.wait_for_envelope_to_be_validated()
        self.submission_manager.submission_envelope.disable_indexing()
        self.submission_manager.submit_envelope()
        self.submission_manager.wait_for_envelope_to_be_validated()

    def complete_run(self, dataset_fixture, project_uuid=None):
        self.dataset = dataset_fixture
        self.upload_spreadsheet_and_create_submission(dataset_fixture, project_uuid=project_uuid)
        self.submission_manager = SubmissionManager(self.submission_envelope)
        self.submission_manager.get_upload_area_credentials()
        self.submission_manager.stage_data_files(self.dataset.config['data_files_location'])
        self.submission_manager.wait_for_envelope_to_be_validated()
        self.submission_manager.submission_envelope.disable_indexing()
        self.submission_manager.submit_envelope()
        self.submission_manager.wait_for_envelope_to_complete()

    def upload_spreadsheet_and_create_submission(self, dataset_fixture, project_uuid=None):
        spreadsheet_filename = os.path.basename(dataset_fixture.metadata_spreadsheet_path)
        Progress.report(f"CREATING SUBMISSION with {spreadsheet_filename}...")
        self.submission_id = self.ingest_broker.upload(dataset_fixture.metadata_spreadsheet_path,
                                                       project_uuid=project_uuid)
        Progress.report(f"submission is in {self.ingest_api.ingest_api_url}/submissionEnvelopes/{self.submission_id}\n")
        self.submission_envelope = self.ingest_api.envelope(self.submission_id)
