import datetime
import json
import os
import time

from ingest.api.ingestapi import IngestApi
from ingest.utils.s2s_token_client import S2STokenClient
from ingest.utils.token_manager import TokenManager

from tests.fixtures.dataset_fixture import DatasetFixture
from tests.ingest_agents import IngestApiAgent, IngestUIAgent
from tests.runners.submission_manager import SubmissionManager
from tests.utils import Progress

METADATA_COUNT = 10


class UpdateSubmissionRunner:
    def __init__(self, deployment):
        self.deployment = deployment

        self.s2s_token_client = S2STokenClient()
        gcp_credentials_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        self.s2s_token_client.setup_from_file(gcp_credentials_file)
        self.token_manager = TokenManager(token_client=self.s2s_token_client)

        self.ingest_broker = IngestUIAgent(deployment=deployment)
        self.ingest_api = IngestApiAgent(deployment=deployment)
        self.ingest_client_api = IngestApi(url=f"https://api.ingest.{self.deployment}.data.humancellatlas.org")

        self.primary_submission = None
        self.update_submission = None

        self.old_values = {}
        self.new_values = {}

    def run(self):
        self.primary_submission = self.run_primary_submission('SS2')

        self.update_submission = self.run_update_submission(self.primary_submission)

        primary_bundles = self.primary_submission.get_bundle_manifests()
        updated_bundles = self.update_submission.get_bundle_manifests()

        #TODO add assertions

    def run_update_submission(self, primary_submission: IngestApiAgent.SubmissionEnvelope):
        token = self.token_manager.get_token()
        self.ingest_client_api.set_token(f'Bearer {token}')
        submission = self.ingest_client_api.create_submission()
        submission_url = submission["_links"]["self"]["href"]
        Progress.report(f"UPDATE submission ID is {submission_url}\n")
        update_submission = self.ingest_api.envelope(url=submission_url)
        update_submission.set_as_update_submission()

        biomaterials = primary_submission.get_biomaterials()

        for biomaterial in biomaterials:
            update_content = dict(biomaterial.get('content'))
            uuid = biomaterial.get('uuid', {}).get('uuid', None)
            update = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ")
            self.old_values[uuid] = update_content["biomaterial_core"]["biomaterial_id"]
            update_content["biomaterial_core"]["biomaterial_id"] = f"updated_donor_id_{update}"
            self.new_values[uuid] = update_content["biomaterial_core"]["biomaterial_id"]

            updated_biomaterial_resource = self.ingest_client_api.createEntity(
                submission_url,
                json.dumps(update_content),
                'biomaterials',
                uuid=uuid)

        submission_manager = SubmissionManager(update_submission)
        submission_manager.wait_for_envelope_to_be_validated()
        submission_manager.submit_envelope()
        submission_manager.wait_for_envelope_to_complete()
        # check old bundle and new bundle
        return update_submission

    def run_primary_submission(self, dataset_name):
        dataset_fixture = DatasetFixture(dataset_name, self.deployment)
        spreadsheet_filename = os.path.basename(dataset_fixture.metadata_spreadsheet_path)
        Progress.report(f"CREATING SUBMISSION with {spreadsheet_filename}...")
        submission_id = self.ingest_broker.upload(dataset_fixture.metadata_spreadsheet_path)
        Progress.report(f"PRIMARY submission ID is {submission_id}\n")
        primary_submission = self.ingest_api.envelope(submission_id)
        submission_manager = SubmissionManager(primary_submission)
        submission_manager.get_upload_area_credentials()
        submission_manager.stage_data_files(dataset_fixture.config['data_files_location'])
        submission_manager.wait_for_envelope_to_be_validated()
        submission_manager.submission_envelope.disable_indexing()
        submission_manager.submit_envelope()
        submission_manager.wait_for_envelope_to_complete()

        return primary_submission
