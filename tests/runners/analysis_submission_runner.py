import json
import os
import time
import uuid

from ingest.api.ingestapi import IngestApi
from ingest.api.requests_utils import create_session_with_retry
from ingest.exporter.bundle import BundleManifest
from ingest.utils.token_manager import TokenManager

from tests.fixtures.analysis_submission_fixture import \
    AnalysisSubmissionFixture
from tests.ingest_agents import IngestUIAgent, IngestApiAgent
from tests.runners.submission_manager import SubmissionManager
from tests.utils import Progress


class AnalysisSubmissionRunner:
    def __init__(self, deployment, ingest_broker: IngestUIAgent, ingest_api: IngestApiAgent,
                 token_manager: TokenManager, ingest_client_api: IngestApi):
        self.deployment = deployment
        self.ingest_broker = ingest_broker
        self.ingest_api = ingest_api
        self.token_manager = token_manager
        self.ingest_client_api = ingest_client_api
        token = self.token_manager.get_token()
        self.ingest_client_api.set_token(f'Bearer {token}')

        self.bundle_manifest_uuid = None
        self.analysis_submission = None
        self.analysis_process = None
        self.analysis_protocol = None
        self.bundle_manifest = None
        self.primary_submission_files = None

        self.analysis_fixture = AnalysisSubmissionFixture()
        self.primary_submission_id = None
        self.primary_submission = None
        self.session = create_session_with_retry()
        self.submission_manager = None

    def run(self, dataset_fixture, analysis_fixture):
        self.create_primary_submission(dataset_fixture)
        time.sleep(20)  # TODO had to add time delay to wait for spreadsheet upload which is async
        self.bundle_manifest_uuid = self.mock_export()
        self.analysis_fixture = analysis_fixture
        self.create_analysis_submission()
        self.submission_manager.wait_for_envelope_to_be_validated()

    def _get_headers(self):
        headers = {'Content-type': 'application/json',
                   'Authorization': f'Bearer {self.token_manager.get_token()}'}
        return headers

    def create_primary_submission(self, dataset_fixture):
        spreadsheet_filename = os.path.basename(
            dataset_fixture.metadata_spreadsheet_path)
        Progress.report(f"CREATING SUBMISSION with {spreadsheet_filename}...")
        self.primary_submission_id = self.ingest_broker.upload(
            dataset_fixture.metadata_spreadsheet_path)
        Progress.report(f"PRIMARY submission ID is {self.primary_submission_id}\n")
        self.primary_submission = self.ingest_api.envelope(self.primary_submission_id)

    def mock_export(self):
        submission_uuid = self.primary_submission.uuid

        bundle_manifest = BundleManifest()
        bundle_manifest.bundleUuid = str(uuid.uuid4())
        bundle_manifest.envelopeUuid = submission_uuid

        bundle_manifest.fileProjectMap = {project['uuid']['uuid']: [project['uuid']['uuid']] for project in
                                          self.primary_submission.get_projects()}
        bundle_manifest.fileBiomaterialMap = {biomaterial['uuid']['uuid']: [biomaterial['uuid']['uuid']] for biomaterial
                                              in self.primary_submission.get_biomaterials()}
        bundle_manifest.fileProcessMap = {process['uuid']['uuid']: [process['uuid']['uuid']] for process in
                                          self.primary_submission.get_processes()}
        bundle_manifest.fileProtocolMap = {protocol['uuid']['uuid']: [protocol['uuid']['uuid']] for protocol in
                                           self.primary_submission.get_protocols()}
        bundle_manifest.fileFilesMap = {file['uuid']['uuid']: [file['uuid']['uuid']] for file in
                                        self.primary_submission.get_files()}
        bundle_manifest.dataFiles = [file['dataFileUuid'] for file in self.primary_submission.get_files()]

        self.ingest_client_api.create_bundle_manifest(bundle_manifest)
        return bundle_manifest.bundleUuid

    def create_analysis_submission(self):
        submission = self.ingest_client_api.create_submission()
        submission_url = submission["_links"]["self"]["href"].rsplit("{")[0]
        Progress.report(f"SECONDARY submission ID is {submission_url}\n")
        self.analysis_submission = self.ingest_api.envelope(envelope_id=None, url=submission_url)
        process = self.ingest_client_api.create_entity(submission_url, self.analysis_fixture.analysis_process,
                                                       'processes')
        protocol = self.ingest_client_api.create_entity(submission_url, self.analysis_fixture.analysis_protocol,
                                                        'protocols')
        input_files = self.primary_submission.get_files()
        self.analysis_process = process
        self.analysis_protocol = protocol
        self.ingest_client_api.link_entity(process, protocol, 'protocols')

        add_input_bundle_url = process['_links']['add-input-bundles']['href']
        input_bundle_uuid = self.bundle_manifest_uuid
        bundle_refs_dict = {'bundleUuids': [input_bundle_uuid]}
        r = self.session.post(add_input_bundle_url, headers=self._get_headers(), json=bundle_refs_dict)
        r.raise_for_status()
        files = self.analysis_fixture.files

        add_input_file_url = process['_links']['inputFiles']['href']
        input_file_uuids = [file['uuid']['uuid'] for file in input_files]
        for file_uuid in input_file_uuids:
            r = self.session.post(add_input_file_url, json.dumps({"inputFileUuid": file_uuid}),
                                  headers=self._get_headers())
            r.raise_for_status()

        add_reference_files_url = process['_links']['add-file-reference']['href']
        for file_content in files:
            analysis_filename = file_content['file_core']['file_name']
            file = {'fileName': analysis_filename, 'content': file_content}
            r = self.session.put(add_reference_files_url, json.dumps(file), headers=self._get_headers())
            r.raise_for_status()

        self.submission_manager = SubmissionManager(self.analysis_submission)
        self.submission_manager.get_upload_area_credentials()
        # TODO restrict permission in the s3 bucket
        # FIXME The following is a workaround because of the issue when uploading files from an s3 bucket. This is
        #  very slow as it's uploading files one at a time, fix this
        # self.submission_manager.stage_data_files('s3://org-humancellatlas-ingest-integration-test/analyses-data')

        self.submission_manager.select_upload_area()
        self.submission_manager.upload_files(
            's3://org-humancellatlas-ingest-integration-test/analysis-data/metrics_summary.csv')
        self.submission_manager.upload_files(
            's3://org-humancellatlas-ingest-integration-test/analysis-data/filtered_gene_bc_matrices_h5.h5')
        self.submission_manager.upload_files(
            's3://org-humancellatlas-ingest-integration-test/analysis-data/molecule_info.h5')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/genes.tsv')
        self.submission_manager.upload_files(
            's3://org-humancellatlas-ingest-integration-test/analysis-data/barcodes.tsv')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/matrix.mtx')
        self.submission_manager.upload_files(
            's3://org-humancellatlas-ingest-integration-test/analysis-data/barcodes.tsv')
        self.submission_manager.upload_files(
            's3://org-humancellatlas-ingest-integration-test/analysis-data/possorted_genome_bam.bam.bai')
        self.submission_manager.upload_files(
            's3://org-humancellatlas-ingest-integration-test/analysis-data/raw_genes.tsv')
        self.submission_manager.upload_files(
            's3://org-humancellatlas-ingest-integration-test/analysis-data/raw_gene_bc_matrices_h5.h5')
        self.submission_manager.upload_files(
            's3://org-humancellatlas-ingest-integration-test/analysis-data/web_summary.html')
        self.submission_manager.upload_files(
            's3://org-humancellatlas-ingest-integration-test/analysis-data/raw_matrix.mtx')
        self.submission_manager.upload_files(
            's3://org-humancellatlas-ingest-integration-test/analysis-data/possorted_genome_bam.bam')
        self.submission_manager.upload_files(
            's3://org-humancellatlas-ingest-integration-test/analysis-data/raw_barcodes.tsv')
        self.submission_manager.forget_about_upload_area()
