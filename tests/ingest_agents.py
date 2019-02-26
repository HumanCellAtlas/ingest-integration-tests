import json
import os
import time

import requests
from ingest.utils.s2s_token_client import S2STokenClient
from ingest.utils.token_manager import TokenManager

from . import logger


class IngestUIAgent:

    INGEST_UI_URL_TEMPLATE = "https://ingest.{}.data.humancellatlas.org"

    def __init__(self, deployment):
        self.deployment = deployment
        self.ingest_broker_url = self.INGEST_UI_URL_TEMPLATE.format(self.deployment)
        self.ingest_auth_agent = IngestAuthAgent()
        self.auth_headers = self.ingest_auth_agent.make_auth_header()

    def upload(self, metadata_spreadsheet_path):
        url = self.ingest_broker_url + '/api_upload'
        files = {'file': open(metadata_spreadsheet_path, 'rb')}
        response = requests.post(url, files=files, allow_redirects=False, headers=self.auth_headers)
        if response.status_code != requests.codes.found and response.status_code != requests.codes.created:
            raise RuntimeError(f"POST {url} response was {response.status_code}: {response.content}")
        return json.loads(response.content)['details']['submission_id']


class IngestApiAgent:

    INGEST_API_URL_TEMPLATE = "http://api.ingest.{}.data.humancellatlas.org"

    def __init__(self, deployment):
        self.deployment = deployment
        self.ingest_api_url = self.INGEST_API_URL_TEMPLATE.format(self.deployment)
        self.ingest_auth_agent = IngestAuthAgent()
        self.auth_headers = self.ingest_auth_agent.make_auth_header()

    def submissions(self):
        url = self.ingest_api_url + '/submissionEnvelopes?size=1000'
        response = requests.get(url, headers=self.auth_headers)
        return response.json()['_embedded']['submissionEnvelopes']

    def envelope(self, envelope_id=None):
        return IngestApiAgent.SubmissionEnvelope(envelope_id=envelope_id, ingest_api_url=self.ingest_api_url,
                                                 auth_headers=self.auth_headers)

    class SubmissionEnvelope:

        def __init__(self, envelope_id=None, ingest_api_url=None, auth_headers=None):
            self.envelope_id = envelope_id
            self.ingest_api_url = ingest_api_url
            self.data = None
            self.auth_headers = auth_headers
            if envelope_id:
                self._load()

        def upload_credentials(self):
            """ Return upload area credentials or None if this envelope doesn't have an upload area yet """
            staging_details = self.data.get('stagingDetails', None)
            if staging_details and 'stagingAreaLocation' in staging_details:
                return staging_details.get('stagingAreaLocation', {}).get('value', None)
            return None

        def reload(self):
            self._load()
            return self

        def status(self):
            return self.data['submissionState']

        def bundles(self):
            url = self.data['_links']['bundleManifests']['href']
            time.sleep(60)  #FIX ME: remove this hacky work around by tuning the backend
            logger.debug('Wait for 60 seconds until "_embedded" field is updated.')
            response = requests.get(url, headers=self.auth_headers).json()
            if '_embedded' in response:
                return [bundleManifest['bundleUuid'] for bundleManifest in response['_embedded']['bundleManifests']]
            else:
                return []

        def _load(self):
            url = self.ingest_api_url + f'/submissionEnvelopes/{self.envelope_id}'
            self.data = requests.get(url, headers=self.auth_headers).json()


class IngestAuthAgent:
    def __init__(self):
        """This class controls the authentication actions with Ingest Service, including retrieving the token,
         store the token and make authenticated headers. Note:
        """
        self.s2s_token_client = S2STokenClient()
        gcp_credentials_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

        self.s2s_token_client.setup_from_file(gcp_credentials_file)
        self.token_manager = TokenManager(token_client=self.s2s_token_client)

    def _get_auth_token(self):
        """Generate self-issued JWT token

        :return string auth_token: OAuth0 JWT token
        """
        auth_token = self.token_manager.get_token()
        return auth_token

    def make_auth_header(self):
        """Make the authorization headers to communicate with endpoints which implement Auth0 authentication API.

        :return dict headers: A header with necessary token information to talk to Auth0 authentication required endpoints.
        """
        headers = {
            "Authorization": f"Bearer {self._get_auth_token()}"
        }
        return headers

