#!/usr/bin/env python3
import os
import unittest

import requests
from ingest.api.ingestapi import IngestApi
from ingest.utils.s2s_token_client import S2STokenClient
from ingest.utils.token_manager import TokenManager

from tests.fixtures.analysis_submission_fixture import AnalysisSubmissionFixture
from tests.fixtures.dataset_fixture import DatasetFixture
from tests.fixtures.metadata_fixture import MetadataFixture
from tests.ingest_agents import IngestUIAgent, IngestApiAgent
from tests.runners.analysis_submission_runner import AnalysisSubmissionRunner
from tests.runners.big_submission_runner import BigSubmissionRunner
from tests.runners.dataset_runner import DatasetRunner
from tests.runners.submission_manager import SubmissionManager
from tests.runners.update_submission_runner import UpdateSubmissionRunner

DEPLOYMENTS = ('dev', 'integration', 'staging')


class TestIngest(unittest.TestCase):

    def setUp(self):
        self.deployment = os.environ.get('DEPLOYMENT_ENV', None)

        if self.deployment not in DEPLOYMENTS:
            raise RuntimeError(f'DEPLOYMENT_ENV environment variable must be one of {DEPLOYMENTS}')

        self.ingest_client_api = IngestApi(url=f"https://api.ingest.{self.deployment}.data.humancellatlas.org")
        self.s2s_token_client = S2STokenClient()
        gcp_credentials_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        self.s2s_token_client.setup_from_file(gcp_credentials_file)
        self.token_manager = TokenManager(self.s2s_token_client)
        self.ingest_broker = IngestUIAgent(self.deployment)
        self.ingest_api = IngestApiAgent(deployment=self.deployment)

    def ingest(self, dataset_name):
        dataset_fixture = DatasetFixture(dataset_name, self.deployment)
        runner = DatasetRunner(self.deployment, self.ingest_broker)
        runner.run(dataset_fixture)
        return runner

    def ingest_data_before_file_metadata(self):
        submission_envelope = self._create_submission_envelope()

        metadata_fixture = MetadataFixture()

        # upload file first
        filename = metadata_fixture.sequence_file['file_core']['file_name']
        submission_manager = SubmissionManager(submission_envelope)
        submission_manager.wait_for_envelope_to_be_in_draft()
        submission_manager.get_upload_area_credentials()
        submission_manager.select_upload_area()
        submission_manager.upload_files(f'{metadata_fixture.data_files_location}{filename}')
        submission_manager.forget_about_upload_area()

        # create metadata


        submission_manager.wait_for_envelope_to_be_validated()

        # upload file
        # wait submission to be valid
        pass

    def _create_submission_envelope(self):
        token = self.token_manager.get_token()
        self.ingest_client_api.set_token(f'Bearer {token}')
        submission = self.ingest_client_api.create_submission()
        submission_url = submission["_links"]["self"]["href"]
        submission_envelope = self.ingest_api.envelope(envelope_id=None, url=submission_url)
        return submission_envelope

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

        self.assertTrue(runner.bundle_manifest_uuid,
                        'The analysis process should be attached to an input bundle manifest')

        derived_files_url = runner.analysis_process['_links']['derivedFiles'][
            'href']
        derived_files = self._get_entities(derived_files_url, 'files')
        analysis_files = runner.analysis_submission.get_files()

        derived_file_uuids = [file['uuid']['uuid'] for file in derived_files]
        analysis_file_uuids = [file['uuid']['uuid'] for file in analysis_files]

        self.assertTrue(derived_file_uuids, 'There must be files in the analysis submission')

        self.assertEqual(derived_file_uuids, analysis_file_uuids,
                         'The analyses files must be linked to the analyses process.')

        input_files_url = runner.analysis_process['_links']['inputFiles'][
            'href']
        input_files = self._get_entities(input_files_url, 'files')
        primary_submission_files = runner.primary_submission.get_files()

        input_file_uuids = [file['uuid']['uuid'] for file in input_files]
        primary_submission_file_uuids = [file['uuid']['uuid'] for file in primary_submission_files]

        self.assertTrue(input_file_uuids, 'There must be files from the primary submission')
        self.assertEqual(input_file_uuids, primary_submission_file_uuids,
                         'The primary submission files must be linked to the analyses process.')

        input_bundle_manifest_url = \
            runner.analysis_process['_links']['inputBundleManifests']['href']
        attached_bundle_manifests = self._get_entities(
            input_bundle_manifest_url, 'bundleManifests')

        self.assertEqual(len(attached_bundle_manifests), 1,
                         'There should only be one input bundle manifest for the analyses process')
        self.assertEqual(attached_bundle_manifests[0]['bundleUuid'],
                         runner.bundle_manifest_uuid,
                         'The input bundle manifest for the analyses process is incorrect')

        return runner

    def ingest_big_submission(self):
        metadata_fixture = MetadataFixture()
        runner = BigSubmissionRunner(self.deployment, self.ingest_client_api, self.token_manager)
        runner.run(metadata_fixture)

    def ingest_updates(self):
        runner = UpdateSubmissionRunner(self.deployment, self.ingest_broker, self.ingest_api, self.ingest_client_api)
        runner.run()

        self.assertEqual(len(runner.updated_bundle_fqids), 1, "There should be 1 bundle updated.")


class TestRun(TestIngest):

    def test_smartseq2_run(self):
        runner = self.ingest('SS2')

    def test_10x_analysis_run(self):
        analysis_runner = self.ingest_analysis('10x')

    def test_big_submission_run(self):
        runner = self.ingest_big_submission()

    def test_updates_run(self):
        runner = self.ingest_updates()

    def test_data_before_file_metadata(self):
        runner = self.ingest_data_before_file_metadata()


if __name__ == '__main__':
    unittest.main()
