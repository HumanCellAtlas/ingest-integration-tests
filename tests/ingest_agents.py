import json
import os
import re
from copy import deepcopy

import requests
from ingest.utils.s2s_token_client import S2STokenClient
from ingest.utils.token_manager import TokenManager

SCHEMA_URL_PATTERN = re.compile('https?://.*/(?P<concrete_type>\\w*)')

class IngestUIAgent:

    INGEST_UI_URL_TEMPLATE = "https://ingest.{}.data.humancellatlas.org"

    def __init__(self, deployment):
        self.deployment = deployment
        self.ingest_broker_url = self.INGEST_UI_URL_TEMPLATE.format(self.deployment)
        self.ingest_auth_agent = IngestAuthAgent()
        self.auth_headers = self.ingest_auth_agent.make_auth_header()

    def upload(self, metadata_spreadsheet_path, is_update=False, project_uuid=None):
        url = self.ingest_broker_url + '/api_upload'
        if is_update:
            url = self.ingest_broker_url + '/api_upload_update'

        data = {}
        if project_uuid:
            data['projectUuid'] = project_uuid
        files = {'file': open(metadata_spreadsheet_path, 'rb')}

        response = requests.post(url, data=data, files=files, allow_redirects=False, headers=self.auth_headers)
        if response.status_code != requests.codes.found and response.status_code != requests.codes.created:
            raise RuntimeError(f"POST {url} response was {response.status_code}: {response.content}")
        return json.loads(response.content)['details']['submission_id']

    def download(self, submission_uuid):
        url = self.ingest_broker_url + f'/submissions/{submission_uuid}/spreadsheet'
        response = requests.get(url)
        return response.content


class IngestApiAgent:

    INGEST_API_URL_TEMPLATE = "https://api.ingest.{}.data.humancellatlas.org"

    def __init__(self, deployment):
        self.deployment = deployment
        self.ingest_api_url = self.INGEST_API_URL_TEMPLATE.format(self.deployment)
        self.ingest_auth_agent = IngestAuthAgent()
        self.auth_headers = self.ingest_auth_agent.make_auth_header()

    def submissions(self):
        url = self.ingest_api_url + '/submissionEnvelopes?size=1000'
        response = requests.get(url, headers=self.auth_headers)
        return response.json()['_embedded']['submissionEnvelopes']

    def envelope(self, envelope_id=None, url=None):
        return IngestApiAgent.SubmissionEnvelope(envelope_id=envelope_id, ingest_api_url=self.ingest_api_url,
                                                 auth_headers=self.auth_headers, url=url)

    class Entity:

        def __init__(self, source: dict = {}):
            self._source = deepcopy(source)

        def get_uuid(self):
            uuid = self._source.get('uuid')
            return uuid.get('uuid') # because uuid's are structured as uuid.uuid in the source JSON

    class SubmissionEnvelope:

        def __init__(self, envelope_id=None, ingest_api_url=None, auth_headers=None, url=None):
            self.envelope_id = envelope_id
            self.url = url
            self.ingest_api_url = ingest_api_url
            self.data = None
            self.auth_headers = auth_headers
            if envelope_id or url:
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

        def submit(self):
            submit_url = self.url + '/submissionEvent'
            r = requests.put(submit_url, headers=self.auth_headers)
            r.raise_for_status()
            return r

        def disable_indexing(self):
            do_not_index = {'triggersAnalysis': False}
            requests.patch(self.url, data=json.dumps(do_not_index))

        def set_as_update_submission(self):
            do_not_index = {'isUpdate': True}
            r = requests.patch(self.url, data=json.dumps(do_not_index), headers=self.auth_headers)
            r.raise_for_status()
            return r

        def get_files(self):
            return self._get_entity_list('files')

        # TODO deprecate this for retrieve_projects; retain get_projects name but use retrieve_projects logic
        def get_projects(self):
            return self._get_entity_list('projects')

        def retrieve_projects(self):
            """
            Similar to get_projects but returns a list of Project objects instead of raw JSON.
            """
            return [IngestApiAgent.Entity(source=source) for source in self.get_projects()]

        def get_protocols(self):
            return self._get_entity_list('protocols')

        def get_processes(self):
            return self._get_entity_list('processes')

        # TODO deprecate for retrieve_biomaterials
        def get_biomaterials(self):
            return self._get_entity_list('biomaterials')

        def retrieve_biomaterials(self):
            return [IngestApiAgent.Entity(source) for source in self.get_biomaterials()]

        def get_bundle_manifests(self):
            return self._get_entity_list('bundleManifests')

        def _get_entity_list(self, entity_type):
            url = self.data['_links'][entity_type]['href']
            r = requests.get(url, headers=self.auth_headers)
            r.raise_for_status()
            files = r.json()
            # TODO won't work for paginated result
            result = files['_embedded'][entity_type] if files.get('_embedded') and files['_embedded'].get(entity_type) else []
            return result


        @property
        def uuid(self):
            return self.data['uuid']['uuid']

        def _load(self):
            if not self.url:
                self.url = self.ingest_api_url + f'/submissionEnvelopes/{self.envelope_id}'

            self.data = requests.get(self.url, headers=self.auth_headers).json()


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

