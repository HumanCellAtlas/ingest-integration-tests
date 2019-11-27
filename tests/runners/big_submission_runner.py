from ingest.api.ingestapi import IngestApi
from ingest.utils.token_manager import TokenManager

from tests.ingest_agents import IngestApiAgent
from tests.runners.submission_manager import SubmissionManager

METADATA_COUNT = 1000


class BigSubmissionRunner:
    def __init__(self, deployment, ingest_client_api: IngestApi, token_manager: TokenManager):
        self.deployment = deployment
        self.ingest_client_api = ingest_client_api
        self.submission_manager = None
        self.submission_envelope = None
        self.ingest_api = IngestApiAgent(deployment=deployment)
        self.token_manager = token_manager

    def run(self, metadata_fixture):
        token = self.token_manager.get_token()
        self.ingest_client_api.set_token(f'Bearer {token}')
        submission = self.ingest_client_api.create_submission()
        submission_url = submission["_links"]["self"]["href"]
        self.submission_envelope = self.ingest_api.envelope(envelope_id=None, url=submission_url)

        biomaterial = metadata_fixture.biomaterial
        file = metadata_fixture.sequence_file
        filename = metadata_fixture.sequence_file['file_core']['file_name']
        self.ingest_client_api.create_file(submission_url, filename, file)

        for i in range(METADATA_COUNT):
            self.ingest_client_api.create_entity(submission_url,
                                                 biomaterial,
                                                 'biomaterials')

        self.submission_manager = SubmissionManager(self.submission_envelope)
        self.submission_manager.wait_for_envelope_to_be_in_draft()
        self.submission_manager.get_upload_area_credentials()
        self.submission_manager.select_upload_area()
        self.submission_manager.upload_files(f'{metadata_fixture.data_files_location}{filename}')
        self.submission_manager.forget_about_upload_area()
        self.submission_manager.wait_for_envelope_to_be_validated()
