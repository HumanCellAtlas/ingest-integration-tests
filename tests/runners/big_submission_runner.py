import json
import os

from ingest.api.ingestapi import IngestApi
from ingest.utils.s2s_token_client import S2STokenClient
from ingest.utils.token_manager import TokenManager

from tests.ingest_agents import IngestApiAgent
from tests.runners.submission_manager import SubmissionManager

METADATA_COUNT = 10000


class BigSubmissionRunner:
    def __init__(self, deployment):
        self.deployment = deployment
        self.ingest_client_api = IngestApi(
            url=f"https://api.ingest.{self.deployment}.data.humancellatlas.org")
        self.s2s_token_client = S2STokenClient()
        gcp_credentials_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        self.s2s_token_client.setup_from_file(gcp_credentials_file)
        self.token_manager = TokenManager(token_client=self.s2s_token_client)
        self.submission_manager = None
        self.submission_envelope = None
        self.ingest_api = IngestApiAgent(deployment=deployment)

    def run(self, metadata_fixture):
        token = self.token_manager.get_token()
        submission_url = self.ingest_client_api.createSubmission(f'Bearer {token}')
        self.submission_envelope = self.ingest_api.envelope(envelope_id=None, url=submission_url)

        biomaterial = json.dumps(metadata_fixture.biomaterial)

        for i in range(METADATA_COUNT):
            self.ingest_client_api.createEntity(submission_url,
                                                      biomaterial,
                                                      'biomaterials')

        self.submission_manager = SubmissionManager(self.submission_envelope)
        self.submission_manager.wait_for_envelope_to_be_validated()
