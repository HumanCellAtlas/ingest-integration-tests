#!/usr/bin/env python3

import os
import subprocess
import unittest

from urllib.parse import urlparse

from .utils import Progress
from .wait_for import WaitFor
from .ingest_agents import IngestUIAgent, IngestApiAgent
from .dataset_fixture import DatasetFixture

DEPLOYMENTS = ('dev', 'integration', 'staging')

MINUTE = 60


class DatasetRunner:

    def __init__(self, deployment):
        self.ingest_broker = IngestUIAgent(deployment=deployment)
        self.ingest_api = IngestApiAgent(deployment=deployment)
        self.submission_id = None
        self.submission_envelope = None
        self.upload_credentials = None
        self.dataset = None
        self.upload_area_uuid = None
        self.deployment = deployment

    def run(self, dataset_fixture):
        self.dataset = dataset_fixture
        self.upload_spreadsheet_and_create_submission(dataset_fixture)
        self.get_upload_area_credentials()
        self.stage_data_files(dataset_fixture)
        self.wait_for_envelope_to_be_validated()

    def upload_spreadsheet_and_create_submission(self, bundle_fixture):
        spreadsheet_filename = os.path.basename(bundle_fixture.metadata_spreadsheet_path)
        Progress.report(f"CREATING SUBMISSION with {spreadsheet_filename}...")
        self.submission_id = self.ingest_broker.upload(bundle_fixture.metadata_spreadsheet_path)
        Progress.report(f" submission ID is {self.submission_id}\n")
        self.submission_envelope = self.ingest_api.envelope(self.submission_id)

    def get_upload_area_credentials(self):
        Progress.report("WAITING FOR STAGING AREA...")
        self.upload_credentials = WaitFor(
            self._get_upload_area_credentials
        ).to_return_a_value_other_than(other_than_value=None, timeout_seconds=2 * MINUTE)
        Progress.report(" credentials received.\n")

    def _get_upload_area_credentials(self):
        return self.submission_envelope.reload().upload_credentials()

    def stage_data_files(self, bundle):
        Progress.report("STAGING FILES...\n")
        self.upload_area_uuid = urlparse(self.upload_credentials).path.split('/')[1]
        self._stage_data_files_using_s3_sync()

    def _stage_data_files_using_s3_sync(self):
        Progress.report("STAGING FILES using hca cli...")
        self.select_upload_area()
        self.upload_files()
        self.forget_about_upload_area()

    def select_upload_area(self):
        upload_area_s3_location = f"s3://org-humancellatlas-upload-{self.deployment}/{self.upload_area_uuid}/"
        self._run_command(['hca', 'upload', 'select', upload_area_s3_location])

    def upload_files(self):
        self._run_command(['hca', 'upload', 'files', self.dataset.config['data_files_location']])

    def forget_about_upload_area(self):
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


class TestEndToEndDCP(unittest.TestCase):

    def setUp(self):
        self.deployment = os.environ.get('CI_COMMIT_REF_NAME', None)

        if self.deployment not in DEPLOYMENTS:
            raise RuntimeError(f"CI_COMMIT_REF_NAME environment variable must be one of {DEPLOYMENTS}")

    def ingest_store_and_analyze_bundle(self, dataset_name):
        dataset_fixture = DatasetFixture(dataset_name, self.deployment)
        runner = DatasetRunner(deployment=self.deployment)
        runner.run(dataset_fixture)
        return runner


class TestSmartSeq2Run(TestEndToEndDCP):

    def test_smartseq2_run(self):
        runner = self.ingest_store_and_analyze_bundle('SS2')


if __name__ == '__main__':
    unittest.main()
