#!/usr/bin/env python3

import os
import re
import subprocess
import unittest

import requests

from .utils import Progress
from .wait_for import WaitFor
from .ingest_agents import IngestUIAgent, IngestApiAgent
from .bundle_fixture import BundleFixture

DEPLOYMENTS = ('dev', 'integration')


class BundleRunner:

    def __init__(self, deployment):
        self.ingest_broker = IngestUIAgent(deployment=deployment)
        self.ingest_api = IngestApiAgent(deployment=deployment)
        self.submission_id = None
        self.submission_envelope = None
        self.upload_credentials = None

    def run(self, bundle_fixture):
        self.upload_spreadsheet_and_create_submission(bundle_fixture)
        self.get_upload_area_credentials()
        self.stage_data_files(bundle_fixture)
        self.forget_about_upload_area()
        self.wait_for_envelope_to_be_validated()
        return self.secondary_bundle_uuid

    def upload_spreadsheet_and_create_submission(self, bundle_fixture):
        spreadhseet_filename = os.path.basename(bundle_fixture.metadata_spreadsheet_path)
        Progress.report(f"CREATING SUBMISSION with {spreadhseet_filename}...")
        self.submission_id = self.ingest_broker.upload(bundle_fixture.metadata_spreadsheet_path)
        Progress.report(f" submission ID is {self.submission_id}\n")
        self.submission_envelope = self.ingest_api.envelope(self.submission_id)

    def get_upload_area_credentials(self):
        Progress.report("WAITING FOR STAGING AREA...")
        self.upload_credentials = WaitFor(
            self._get_upload_area_credentials
        ).to_return_a_value_other_than(other_than_value=None, timeout_seconds=60)
        Progress.report(" credentials received.\n")

    def _get_upload_area_credentials(self):
        return self.submission_envelope.reload().upload_credentials()

    def stage_data_files(self, bundle):
        Progress.report("STAGING FILES...\n")
        self._run_command(['hca', 'upload', 'select', self.upload_credentials])
        for file_path in bundle.data_files_paths():
            self._run_command(['hca', 'upload', 'file', file_path])

    def forget_about_upload_area(self):
        upload_area_uuid = self.upload_credentials.split(':')[4]
        self._run_command(['hca', 'upload', 'forget', upload_area_uuid])

    def wait_for_envelope_to_be_validated(self):
        Progress.report("WAIT FOR VALIDATION...")
        WaitFor(self._envelope_is_valid).to_return_value(value=True, timeout_seconds=10 * 60)
        Progress.report(" envelope is valid.\n")

    def _envelope_is_valid(self):
        envelope_status = self.submission_envelope.reload().status()
        Progress.report(f"envelope status is {envelope_status}")
        return envelope_status in ['Valid', 'Submitted']

    @staticmethod
    def _run_command(cmd_and_args_list, expected_retcode=0):
        retcode = subprocess.call(cmd_and_args_list)
        if retcode != 0:
            raise Exception(
                "Unexpected return code from '{command}', expected {expected_retcode} got {actual_retcode}".format(
                    command=" ".join(cmd_and_args_list), expected_retcode=expected_retcode, actual_retcode=retcode
                )
            )


class TestEndToEndDCP(unittest.TestCase):

    def setUp(self):
        self.deployment = os.environ.get('TRAVIS_BRANCH', None)
        if self.deployment not in DEPLOYMENTS:
            raise RuntimeError(f"TRAVIS_BRANCH environment variable must be one of {DEPLOYMENTS}")

    def ingest_store_and_analyze_bundle(self, bundle_fixture_path):
        bundle_fixture = BundleFixture(bundle_fixture_path)
        runner = BundleRunner(deployment=self.deployment)
        runner.run(bundle_fixture)
        return runner


class TestSmartSeq2Run(TestEndToEndDCP):

    SMARTSEQ2_BUNDLE_FIXTURE_PATH = 'tests/fixtures/bundles/Smart-seq2'

    def test_smartseq2_run(self):
        runner = self.ingest_store_and_analyze_bundle(self.SMARTSEQ2_BUNDLE_FIXTURE_PATH)


if __name__ == '__main__':
    unittest.main()
