#!/usr/bin/env python3
import os
import unittest

import requests

from tests.fixtures.analysis_submission_fixture import AnalysisSubmissionFixture
from tests.fixtures.metadata_fixture import MetadataFixture
from tests.runners.analysis_submission_runner import AnalysisSubmissionRunner
from tests.runners.big_submission_runner import BigSubmissionRunner
from tests.runners.update_submission_runner import UpdateSubmissionRunner
from tests.runners.dataset_runner import DatasetRunner
from tests.fixtures.dataset_fixture import DatasetFixture

DEPLOYMENTS = ('dev', 'integration', 'staging')


class TestIngest(unittest.TestCase):

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

        self.assertTrue(runner.bundle_manifest_uuid, 'The analysis process should be attached to an input bundle manifest')

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
        runner = BigSubmissionRunner(self.deployment)
        runner.run(metadata_fixture)

    def ingest_updates(self):
        runner = UpdateSubmissionRunner(self.deployment)
        runner.run()

        self.assertEqual(len(runner.updated_bundle_fqids), 2, "There should be 1 bundle updated.")


class TestRun(TestIngest):

    def test_smartseq2_run(self):
        runner = self.ingest('SS2')

    def test_10x_analysis_run(self):
        analysis_runner = self.ingest_analysis('10x')

    def test_big_submission_run(self):
        runner = self.ingest_big_submission()

    def test_updates_run(self):
        runner = self.ingest_updates()


if __name__ == '__main__':
    unittest.main()