import json
import os
import time
import uuid

from ingest.api.ingestapi import IngestApi, BundleManifest
from ingest.utils.s2s_token_client import S2STokenClient
from ingest.utils.token_manager import TokenManager

from tests.ingest_agents import IngestApiAgent
from tests.runners.submission_manager import SubmissionManager

METADATA_COUNT = 10


class UpdateSubmissionRunner:
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
        self.bundle_manifest = None

    def run(self, metadata_fixture):
        token = self.token_manager.get_token()
        self.ingest_client_api.set_token(f'Bearer {token}')
        submission = self.ingest_client_api.create_submission()
        submission_url = submission["_links"]["self"]["href"]

        self.submission_envelope = self.ingest_api.envelope(envelope_id=None, url=submission_url)
        biomaterial = json.dumps(metadata_fixture.biomaterial)
        created_metadata_uuids = []
        canonical_document_urls = []
        for i in range(METADATA_COUNT):
            created_biomaterial = self.ingest_client_api.createEntity(submission_url,
                                                                      biomaterial,
                                                                      'biomaterials')
            created_metadata_uuids.append(created_biomaterial["uuid"]["uuid"])
            canonical_document_urls.append(created_biomaterial["_links"]["self"]["href"])


        self.submission_manager = SubmissionManager(self.submission_envelope)
        self.submission_manager.wait_for_envelope_to_be_validated()

        self.bundle_manifest = self.mock_export()

        update_submission_resource= self.ingest_client_api.create_submission(True)
        update_submission_url = update_submission_resource["_links"]["self"]["href"]
        update_submission_envelope = self.ingest_api.envelope(envelope_id=None, url=update_submission_url)
        updated_biomaterial = json.loads(biomaterial)
        updated_biomaterial["biomaterial_core"]["biomaterial_id"] = "updated_donor_id"

        for uuid in created_metadata_uuids:
            updated_biomaterial_resource = self.ingest_client_api.createEntity(update_submission_url,
                                                                               json.dumps(updated_biomaterial),
                                                                               'biomaterials',
                                                                               uuid=uuid)

        self.submission_manager = SubmissionManager(update_submission_envelope)
        self.submission_manager.wait_for_envelope_to_be_validated()
        self.submission_manager.submit_envelope()
        self.submission_manager.wait_for_envelope_to_be_submitted()

        time.sleep(5) # sleeping here because upserts happen asynchronously post-submit
        # TODO: modify this test to wait_for_envelope_to_be_processing
        for canonical_document_resource in self.ingest_client_api.getEntities(submission_url, "biomaterials"):
            assert canonical_document_resource["content"]["biomaterial_core"]["biomaterial_id"] == "updated_donor_id"


    def mock_export(self):
        submission_uuid = self.submission_envelope.uuid

        bundle_manifest = BundleManifest()
        bundle_manifest.bundleUuid = str(uuid.uuid4())
        bundle_manifest.envelopeUuid = submission_uuid

        bundle_manifest.fileProjectMap = {project['uuid']['uuid']: [project['uuid']['uuid']] for project in self.submission_envelope.get_projects()}
        bundle_manifest.fileBiomaterialMap = {biomaterial['uuid']['uuid']: [biomaterial['uuid']['uuid']] for biomaterial in self.submission_envelope.get_biomaterials()}
        bundle_manifest.fileProcessMap = {process['uuid']['uuid']: [process['uuid']['uuid']] for process in self.submission_envelope.get_processes()}
        bundle_manifest.fileProtocolMap = {protocol['uuid']['uuid']: [protocol['uuid']['uuid']] for protocol in self.submission_envelope.get_protocols()}
        bundle_manifest.fileFilesMap = {file['uuid']['uuid']: [file['uuid']['uuid']] for file in self.submission_envelope.get_files()}
        bundle_manifest.dataFiles = [file['dataFileUuid'] for file in self.submission_envelope.get_files()]

        self.ingest_client_api.createBundleManifest(bundle_manifest)
        return bundle_manifest
