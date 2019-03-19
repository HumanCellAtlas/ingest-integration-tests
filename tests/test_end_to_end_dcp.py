#!/usr/bin/env python3
import json
import os
import subprocess
import unittest
import time
import requests.packages.urllib3.util.retry as retry

from urllib.parse import urlparse

import requests
from ingest.utils.s2s_token_client import S2STokenClient
from ingest.utils.token_manager import TokenManager
from requests import Session

from tests.analysis_submission_fixture import AnalysisSubmissionFixture
from .utils import Progress
from .wait_for import WaitFor
from .ingest_agents import IngestUIAgent, IngestApiAgent, IngestAuthAgent
from .dataset_fixture import DatasetFixture
from ingest.api.ingestapi import IngestApi, BundleManifest

DEPLOYMENTS = ('dev', 'integration', 'staging')

MINUTE = 60


class SubmissionManager:

    def __init__(self, submission_envelope):
        self.submission_envelope = submission_envelope
        self.upload_credentials = None

    def get_upload_area_credentials(self):
        Progress.report("WAITING FOR STAGING AREA...")
        self.upload_credentials = WaitFor(
            self._get_upload_area_credentials
        ).to_return_a_value_other_than(other_than_value=None, timeout_seconds=2 * MINUTE)
        Progress.report(" credentials received.\n")

    def _get_upload_area_credentials(self):
        return self.submission_envelope.reload().upload_credentials()

    def stage_data_files(self, files):
        Progress.report("STAGING FILES...\n")
        self._stage_data_files_using_s3_sync(files)

    def _stage_data_files_using_s3_sync(self, files):
        Progress.report("STAGING FILES using hca cli...")
        self.select_upload_area()
        self.upload_files(files)
        self.forget_about_upload_area()

    def select_upload_area(self):
        self._run_command(['hca', 'upload', 'select', self.upload_credentials])

    def upload_files(self, files):
        self._run_command(['hca', 'upload', 'files', files])

    def forget_about_upload_area(self):
        self.upload_area_uuid = urlparse(self.upload_credentials).path.split('/')[1]
        self._run_command(['hca', 'upload', 'forget', self.upload_area_uuid])

    def wait_for_envelope_to_be_validated(self):
        Progress.report("WAIT FOR VALIDATION...")
        WaitFor(self._envelope_is_valid).to_return_value(value=True)
        Progress.report(" envelope is valid.\n")

    def _envelope_is_valid(self):
        envelope_status = self.submission_envelope.reload().status()
        Progress.report(f"envelope status is {envelope_status}")
        return envelope_status in ['Valid']

    @staticmethod
    def _run_command(cmd_and_args_list, expected_retcode=0):
        retcode = subprocess.call(cmd_and_args_list)
        if retcode != 0:
            raise Exception(
                "Unexpected return code from '{command}', expected {expected_retcode} got {actual_retcode}".format(
                    command=" ".join(cmd_and_args_list), expected_retcode=expected_retcode, actual_retcode=retcode
                )
            )


class DatasetRunner:

    def __init__(self, deployment, export_bundles=False):
        self._export_bundle = export_bundles
        self.deployment = deployment

        self.ingest_broker = IngestUIAgent(deployment=deployment)
        self.ingest_api = IngestApiAgent(deployment=deployment)
        self.submission_id = None
        self.submission_envelope = None

        self.dataset = None

        self.submission_manager = None

    def run(self, dataset_fixture):
        self.dataset = dataset_fixture
        self.upload_spreadsheet_and_create_submission(dataset_fixture)
        self.submission_manager = SubmissionManager(self.submission_envelope)
        self.submission_manager.get_upload_area_credentials()
        self.submission_manager.stage_data_files(self.dataset.config['data_files_location'])
        self.submission_manager.wait_for_envelope_to_be_validated()

    def upload_spreadsheet_and_create_submission(self, bundle_fixture):
        spreadsheet_filename = os.path.basename(bundle_fixture.metadata_spreadsheet_path)
        Progress.report(f"CREATING SUBMISSION with {spreadsheet_filename}...")
        self.submission_id = self.ingest_broker.upload(bundle_fixture.metadata_spreadsheet_path)
        Progress.report(f" submission ID is {self.submission_id}\n")
        self.submission_envelope = self.ingest_api.envelope(self.submission_id)


class AnalysisSubmissionRunner:
    def __init__(self, deployment):
        self.deployment = deployment
        self.ingest_broker = IngestUIAgent(deployment=deployment)
        self.ingest_client_api = IngestApi(url=f"https://api.ingest.{self.deployment}.data.humancellatlas.org")
        self.ingest_api = IngestApiAgent(deployment=deployment)
        self.s2s_token_client = S2STokenClient()
        gcp_credentials_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        self.s2s_token_client.setup_from_file(gcp_credentials_file)
        self.token_manager = TokenManager(token_client=self.s2s_token_client)

        self.bundle_manifest_uuid = None
        self.analysis_submission = None
        self.analysis_process = None
        self.analysis_protocol = None
        self.bundle_manifest = None
        self.primary_submission_files = None

        self.analysis_fixture = AnalysisSubmissionFixture()
        self.primary_submission_id = None
        self.primary_submission = None

        self.submission_manager = None
        self.session = create_session_with_retry()

    def run(self, dataset_fixture, analysis_fixture):
        self.create_primary_submission(dataset_fixture)
        time.sleep(20)  # TODO had to add time delay to wait for spreadsheet upload which is async
        self.bundle_manifest_uuid = self.mock_export()
        self.analysis_fixture = analysis_fixture
        self.create_analysis_submission()

    def create_primary_submission(self, dataset_fixture):
        spreadsheet_filename = os.path.basename(
            dataset_fixture.metadata_spreadsheet_path)
        Progress.report(f"CREATING SUBMISSION with {spreadsheet_filename}...")
        self.primary_submission_id = self.ingest_broker.upload(
            dataset_fixture.metadata_spreadsheet_path)
        Progress.report(f" submission ID is {self.primary_submission_id}\n")
        self.primary_submission = self.ingest_api.envelope(self.primary_submission_id)

    def mock_export(self):
        submission_uuid = self.primary_submission.uuid

        bundle_manifest = BundleManifest()
        bundle_manifest.envelopeUuid = submission_uuid

        bundle_manifest.fileProjectMap = {project['uuid']['uuid']: [project['uuid']['uuid']] for project in self.primary_submission.get_projects()}
        bundle_manifest.fileBiomaterialMap = {biomaterial['uuid']['uuid']: [biomaterial['uuid']['uuid']] for biomaterial in self.primary_submission.get_biomaterials()}
        bundle_manifest.fileProcessMap = {process['uuid']['uuid']: [process['uuid']['uuid']] for process in self.primary_submission.get_processes()}
        bundle_manifest.fileProtocolMap = {protocol['uuid']['uuid']: [protocol['uuid']['uuid']] for protocol in self.primary_submission.get_protocols()}
        bundle_manifest.fileFilesMap = {file['uuid']['uuid']: [file['uuid']['uuid']] for file in self.primary_submission.get_files()}
        bundle_manifest.dataFiles = [file['dataFileUuid'] for file in self.primary_submission.get_files()]

        self.ingest_client_api.createBundleManifest(bundle_manifest)
        return bundle_manifest.bundleUuid

    def create_analysis_submission(self, ):
        token = self.token_manager.get_token()
        submission_url = self.ingest_client_api.createSubmission(f'Bearer {token}')

        self.analysis_submission = self.ingest_api.envelope(envelope_id=None, url=submission_url)
        process = self.ingest_client_api.createEntity(submission_url, json.dumps(self.analysis_fixture.analysis_process), 'processes')
        protocol = self.ingest_client_api.createEntity(submission_url, json.dumps(self.analysis_fixture.analysis_protocol), 'protocols')
        self.analysis_process = process
        self.analysis_protocol = protocol

        process_url = process['_links']['self']['href']
        protocol_url = protocol['_links']['self']['href']

        link_protocols_url = process['_links']['protocols']['href']

        add_input_bundle_url = process['_links']['add-input-bundles']['href']
        add_reference_files_url = process['_links']['add-file-reference']['href']

        headers = {"Content-Type": "text/uri-list"}
        r = self.session.put(link_protocols_url, headers=headers, data=protocol_url)
        r.raise_for_status()

        input_bundle_uuid = self.bundle_manifest_uuid
        bundle_refs_dict = {'bundleUuids': [input_bundle_uuid]}
        r = self.session.put(add_input_bundle_url, headers={'Content-type': 'application/json'},
                                     json=bundle_refs_dict)
        r.raise_for_status()

        files = self.analysis_fixture.files

        for file_content in files:
            analysis_filename = file_content['file_core']['file_name']
            file = {'fileName': analysis_filename, 'content': file_content}
            r = requests.put(add_reference_files_url, json.dumps(file), headers={'Content-type': 'application/json'})
            r.raise_for_status()

        self.submission_manager = SubmissionManager(self.analysis_submission)
        self.submission_manager.get_upload_area_credentials()
        # TODO restrict permission in the s3 bucket
        # FIXME The following is a workaround because of the issue when uploading files from an s3 bucket. This is very slow as it's uploading files one at a time, fix this
        # self.submission_manager.stage_data_files('s3://org-humancellatlas-ingest-integration-test/analysis-data')

        self.submission_manager.select_upload_area()
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/metrics_summary.csv')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/filtered_gene_bc_matrices_h5.h5')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/molecule_info.h5')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/genes.tsv')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/barcodes.tsv')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/matrix.mtx')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/barcodes.tsv')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/possorted_genome_bam.bam.bai')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/raw_genes.tsv')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/raw_gene_bc_matrices_h5.h5')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/web_summary.html')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/raw_matrix.mtx')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/possorted_genome_bam.bam')
        self.submission_manager.upload_files('s3://org-humancellatlas-ingest-integration-test/analysis-data/raw_barcodes.tsv')
        self.submission_manager.forget_about_upload_area()
        self.submission_manager.wait_for_envelope_to_be_validated()


def create_session_with_retry() -> Session:
    retry_policy = retry.Retry(
        total=100,
        # seems that this has a default value of 10,
        # setting this to a very high number so that it'll respect the status retry count

        status=17,
        # status is the no. of retries if response is in status_forcelist,
        # this count will retry for ~20 mins with back off timeout within

        read=10,
        status_forcelist=[500, 502, 503, 504, 409],
        backoff_factor=0.6,
        method_whitelist=frozenset(
            ['HEAD', 'GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'TRACE'])
    )
    session = Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=retry_policy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


class TestEndToEndDCP(unittest.TestCase):

    def setUp(self):
        self.deployment = os.environ.get('CI_COMMIT_REF_NAME', None)

        if self.deployment not in DEPLOYMENTS:
            raise RuntimeError(f'CI_COMMIT_REF_NAME environment variable must be one of {DEPLOYMENTS}')

    def ingest(self, dataset_name):
        dataset_fixture = DatasetFixture(dataset_name, self.deployment)
        runner = DatasetRunner(deployment=self.deployment)
        runner.run(dataset_fixture)
        return runner

    # TODO move this to ingest client api
    def _get_entities(self, url, entity_type):
        r = requests.get(url, headers={'Content-type': 'application/json'})
        r.raise_for_status()
        response = r.json()

        if response.get('_embedded') and response['_embedded'].get(entity_type):
            return response['_embedded'][entity_type]
        else:
            return []

    def ingest_analysis(self, dataset_name):
        analysis_fixture = AnalysisSubmissionFixture()
        runner = AnalysisSubmissionRunner(deployment=self.deployment)
        dataset_fixture = DatasetFixture(dataset_name, self.deployment)
        runner.run(dataset_fixture, analysis_fixture)

        derived_files_url = runner.analysis_process['_links']['derivedFiles'][
            'href']
        derived_files = self._get_entities(derived_files_url, 'files')
        analysis_files = runner.analysis_submission.get_files()

        self.assertEqual(derived_files, analysis_files,
                         'The analysis files must be linked to the analysis process.')

        input_files_url = runner.analysis_process['_links']['inputFiles'][
            'href']
        input_files = self._get_entities(input_files_url, 'files')
        primary_submission_files = runner.primary_submission.get_files()

        self.assertEqual(input_files, primary_submission_files,
                         'The primary submission files must be linked to the analysis process.')

        input_bundle_manifest_url = \
            runner.analysis_process['_links']['inputBundleManifests']['href']
        attached_bundle_manifests = self._get_entities(
            input_bundle_manifest_url, 'bundleManifests')

        self.assertEqual(len(attached_bundle_manifests), 1,
                         'There should only be one input bundle manifest for the analysis process')
        self.assertEqual(attached_bundle_manifests[0]['bundleUuid'],
                         runner.bundle_manifest_uuid,
                         'The input bundle manifest for the analysis process is incorrect')

        return runner


class TestRun(TestEndToEndDCP):

    def test_smartseq2_run(self):
        runner = self.ingest('SS2')

    def test_10x_analysis_run(self):
        analysis_runner = self.ingest_analysis('10x')


if __name__ == '__main__':
    unittest.main()
