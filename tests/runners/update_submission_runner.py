import datetime
import os

import openpyxl
from ingest.api.ingestapi import IngestApi
from ingest.utils.s2s_token_client import S2STokenClient
from ingest.utils.token_manager import TokenManager

from tests.fixtures.dataset_fixture import DatasetFixture
from tests.ingest_agents import IngestApiAgent, IngestUIAgent
from tests.runners.submission_manager import SubmissionManager
from tests.utils import Progress

METADATA_COUNT = 10


class BundleManifest:
    def __init__(self, resource):
        self._object = resource

    @property
    def fqid(self):
        return f'{self.uuid}.{self.version}'

    @property
    def version(self):
        return self._object.get('bundleVersion')

    @property
    def uuid(self):
        return self._object.get('bundleUuid')


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

        self.primary_bundle_fqids = None
        self.updated_bundle_fqids = None

    def run(self):
        self.primary_submission = self.run_primary_submission('SS2')
        primary_bundle_manifests = self.primary_submission.get_bundle_manifests()
        self.primary_bundle_fqids = [BundleManifest(obj).fqid for obj in primary_bundle_manifests]
        projects = self.primary_submission.get_projects()

        self.update_submission = self.run_update_submission(self.primary_submission)
        updated_bundle_manifests = self.update_submission.get_bundle_manifests()
        self.updated_bundle_fqids = [BundleManifest(obj).fqid for obj in updated_bundle_manifests]

        Progress.report(f"PROJECT UUID: {projects[0]['uuid']['uuid']}")
        Progress.report(f"PRIMARY BUNDLES: {' '.join(self.primary_bundle_fqids)}")
        Progress.report(f"UPDATE BUNDLES: {' '.join(self.updated_bundle_fqids)}")

        return self

    def run_update_submission(self, primary_submission: IngestApiAgent.SubmissionEnvelope):
        update_spreadsheet_content = self.ingest_broker.download(primary_submission.uuid)
        update_spreadsheet_filename = f'{primary_submission.uuid}.xlsx'
        update_spreadsheet_path = os.path.abspath(os.path.join(os.path.dirname(__file__),update_spreadsheet_filename))
        with open(update_spreadsheet_path, 'wb') as f:
            f.write(update_spreadsheet_content)

        update_spreadsheet = openpyxl.load_workbook(update_spreadsheet_path)
        project_worksheet = update_spreadsheet['Project']
        if project_worksheet['B4'].value != "project.project_core.project_short_name":
            raise RuntimeError("Project shortname is no longer in cell project!B4")
        project_worksheet['B6'] = f"UPDATED {project_worksheet['B6'].value}"
        update_spreadsheet.save(update_spreadsheet_path)

        update_submission_id = self.ingest_broker.upload(update_spreadsheet_path, is_update=True)
        Progress.report(f"UPDATE submission ID is {update_submission_id}\n")
        update_submission = self.ingest_api.envelope(envelope_id=update_submission_id)

        submission_manager = SubmissionManager(update_submission)
        submission_manager.wait_for_envelope_to_be_validated()
        submission_manager.submit_envelope()
        submission_manager.wait_for_envelope_to_be_submitted()
        submission_manager.wait_for_envelope_to_complete()
        # check old bundle and new bundle
        return update_submission

    def run_primary_submission(self, dataset_name):
        dataset_fixture = DatasetFixture(dataset_name, self.deployment)
        spreadsheet_filename = os.path.basename(dataset_fixture.metadata_spreadsheet_path)
        Progress.report(f"CREATING SUBMISSION with {spreadsheet_filename}...")
        submission_id = self.ingest_broker.upload(dataset_fixture.metadata_spreadsheet_path)
        Progress.report(f"PRIMARY submission is in {self.ingest_api.ingest_api_url}/submissionEnvelopes/{submission_id}\n")
        primary_submission = self.ingest_api.envelope(submission_id)

        submission_manager = SubmissionManager(primary_submission)
        submission_manager.get_upload_area_credentials()
        submission_manager.stage_data_files(dataset_fixture.config['data_files_location'])
        submission_manager.wait_for_envelope_to_be_validated()

        # Disable indexing since this is an internal test for ingest, we don't need to trigger analysis pipelines
        submission_manager.submission_envelope.disable_indexing()

        submission_manager.submit_envelope()
        submission_manager.wait_for_envelope_to_complete()

        return primary_submission

