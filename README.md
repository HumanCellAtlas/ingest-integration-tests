[![pipeline status](https://allspark.dev.data.humancellatlas.org/HumanCellAtlas/ingest-integration-tests/badges/dev/pipeline.svg)](https://allspark.dev.data.humancellatlas.org/HumanCellAtlas/ingest-integration-tests/commits/dev)

_This repository was part of HCA DCP/1 and is not maintained anymore. DCP/2 development of this component continues in the forked repository at https://github.com/ebi-ait/ingest-integration-tests._

# Ingest Integration Tests
Integration tests for ingest and upload run in non-production environments.

## Developer Notes

### Running Locally

#### Local Shell Environment

##### Setting Up the Environment

The tests require a Python 3 environment to run. All the required modules are listed in the `requirements.txt` and can
be installed through `pip`:

    pip install -r requirements.txt
    
For the tests to be able to successfully communicate with other external services, the GCP credentials need to be 
made locally available. The GCP credentials are stored in AWS Secrets Manager; one set is store for each development 
environment. To retrieve GCP credentials, the AWS CLI can be used:

```
aws --profile=hca secretsmanager get-secret-value\
    --region us-east-1\ 
    --secret-id dcp/ingest/dev/gcp-credentials.json\
    --query SecretString | jq -rc > _local/gcp-credentials-dev.json
```

**IMPORTANT**: Store the credentials file in a secured location. Make sure to not commit it to version control. 
The `_local` directory given as an example above is a special directory that is configured to be automatically 
ignored by the version control system.

##### Running a Single Test

To run a single test, make sure that all necessary environment variables are provided. At the time of writing, the most
commonly required variables are `DEPLOYMENT_ENV` and `GOOGLE_APPLICATION_CREDENTIALS`.

```
export DEPLOYMENT_ENV=dev; \
export GOOGLE_APPLICATION_CREDENTIALS=_local/gcp-credentials-dev.json; \
python -m unittest tests.test_add_bundle.AddBundleTest.test_run
``` 

#### Gitlab Runner

The integration tests are primarily designed to run through the Gitlab CI/CD pipeline mechanism. The tests can be run
through Gitlab locally using `gitlab-runner` that can either be installed or be run as Docker container. Please refer
to [the officially documentation](https://docs.gitlab.com/runner/) for more information.
