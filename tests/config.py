import os

from tests.ingest_agents import IngestUIAgent

deployment = os.environ.get('DEPLOYMENT_ENV', None)
ingest_broker = IngestUIAgent(deployment)