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

    def run(self):
        self.primary_submission = self.run_primary_submission('SS2')
        self.update_submission = self.run_update_submission(self.primary_submission)

        time.sleep(5)  # sleeping here because upserts happen asynchronously post-submit
        # TODO: modify this test to wait_for_envelope_to_be_processing
        for canonical_document_resource in self.ingest_client_api.getEntities(
                self.primary_submission.url, "biomaterials"):
            assert canonical_document_resource["content"]["biomaterial_core"][
                       "biomaterial_id"] == "updated_donor_id"

    def run_update_submission(self, primary_submission: IngestApiAgent.SubmissionEnvelope):
        token = self.token_manager.get_token()
        self.ingest_client_api.set_token(f'Bearer {token}')
        submission = self.ingest_client_api.create_submission()
        submission_url = submission["_links"]["self"]["href"]

        update_submission = self.ingest_api.envelope(url=submission_url)
        update_submission.set_as_update_submission()

        biomaterials = primary_submission.get_biomaterials()

        for biomaterial in biomaterials:
            update_content = dict(biomaterial.get('content'))
            uuid = biomaterial.get('uuid', {}).get('uuid', None)
            url = biomaterial['_links']['self']['href']
            update_content["biomaterial_core"]["biomaterial_id"] = "updated_donor_id"
            updated_biomaterial_resource = self.ingest_client_api.createEntity(
                url,
                json.dumps(update_content),
                'biomaterials',
                uuid=uuid)

        self.submission_manager = SubmissionManager(update_submission)
        self.submission_manager.wait_for_envelope_to_be_validated()
        self.submission_manager.submit_envelope()
        self.submission_manager.wait_for_envelope_to_complete()
        return update_submission

    def run_primary_submission(self, dataset_name):
        dataset_fixture = DatasetFixture(dataset_name, self.deployment)
        spreadsheet_filename = os.path.basename(dataset_fixture.metadata_spreadsheet_path)
        Progress.report(f"CREATING SUBMISSION with {spreadsheet_filename}...")
        submission_id = self.ingest_broker.upload(dataset_fixture.metadata_spreadsheet_path)
        Progress.report(f" submission ID is {submission_id}\n")
        primary_submission = self.ingest_api.envelope(self.submission_id)
        self.upload_spreadsheet_and_create_submission(dataset_fixture)
        submission_manager = SubmissionManager(self.submission_envelope)
        submission_manager.get_upload_area_credentials()
        submission_manager.stage_data_files(self.dataset.config['data_files_location'])
        submission_manager.wait_for_envelope_to_be_validated()
        submission_manager.submission_envelope.disable_indexing()
        submission_manager.submit_envelope()
        submission_manager.wait_for_envelope_to_complete()

        return primary_submission
