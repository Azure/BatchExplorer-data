from __future__ import print_function
from azure.common.credentials import ServicePrincipalCredentials
import azure.batch.batch_service_client as batch
import traceback
import datetime
import os
import sys
import json
import asyncio
import JobManager
import Utils
import azure.storage.blob as azureblob
import azure.batch.models as batchmodels
import azext.batch as batch
import argparse

 
"""
This python module is used for valiading the rendering templates by using the azure CLI. 
This module will load the manifest file 'TestConfiguration' specified by the user and
create pools and jobs based on this file.
"""

sys.path.append('.')
sys.path.append('..')

_BATCH_ACCOUNT_NAME = os.environ['PS_BATCH_ACCOUNT_NAME']
_BATCH_ACCOUNT_KEY = os.environ['PS_BATCH_ACCOUNT_KEY']
_BATCH_ACCOUNT_URL = os.environ['PS_BATCH_ACCOUNT_URL']
_BATCH_ACCOUNT_SUB = os.environ['PS_BATCH_ACCOUNT_SUB']
_STORAGE_ACCOUNT_NAME = os.environ['PS_STORAGE_ACCOUNT_NAME']
_STORAGE_ACCOUNT_KEY = os.environ['PS_STORAGE_ACCOUNT_KEY']
_SERVICE_PRINCIPAL_CREDENTIALS_CLIENT_ID = os.environ['PS_SERVICE_PRINCIPAL_CREDENTIALS_CLIENT_ID']
_SERVICE_PRINCIPAL_CREDENTIALS_SECRET = os.environ['PS_SERVICE_PRINCIPAL_CREDENTIALS_SECRET']
_SERVICE_PRINCIPAL_CREDENTIALS_TENANT = os.environ['PS_SERVICE_PRINCIPAL_CREDENTIALS_TENANT']
_SERVICE_PRINCIPAL_CREDENTIALS_RESOUCE = os.environ['PS_SERVICE_PRINCIPAL_CREDENTIALS_RESOUCE']

_timeout = 20
_job_managers = []


if __name__ == '__main__':
    start_time = datetime.datetime.now().replace(microsecond=0)
    Utils.logger.info('Template runner start time: {}'.format(start_time))
    Utils.logger.info("_BATCH_ACCOUNT_NAME: {}".format(_BATCH_ACCOUNT_NAME))
    Utils.logger.info("_BATCH_ACCOUNT_URL: {}".format(_BATCH_ACCOUNT_URL))
    Utils.logger.info("_STORAGE_ACCOUNT_NAME: {}".format(_STORAGE_ACCOUNT_NAME))

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    blob_client = azureblob.BlockBlobService(
        account_name=_STORAGE_ACCOUNT_NAME,
        account_key=_STORAGE_ACCOUNT_KEY)

    credentials = ServicePrincipalCredentials(
        client_id=_SERVICE_PRINCIPAL_CREDENTIALS_CLIENT_ID,
        secret=_SERVICE_PRINCIPAL_CREDENTIALS_SECRET,
        tenant=_SERVICE_PRINCIPAL_CREDENTIALS_TENANT,
        resource=_SERVICE_PRINCIPAL_CREDENTIALS_RESOUCE)

    batch_client = batch.BatchExtensionsClient(
        credentials=credentials,
        batch_account=_BATCH_ACCOUNT_NAME,
        base_url=_BATCH_ACCOUNT_URL,
        subscription_id=_BATCH_ACCOUNT_SUB)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "TestConfig",
        help="A manifest file that contains a list of all the jobs and pools you want to create.")
    args = parser.parse_args()

    # "Tests/TestConfiguration.json"
    jsonConfigFile = args.TestConfig

    # Clean up any storage container that is older than a 7 days old.
    try:
        Utils.cleanup_old_resources(blob_client)
    except Exception as e:
        raise e

    try:
        with open(jsonConfigFile) as f:
            template = json.load(f)

        for i in range(0, len(template["tests"])):
            jobSettings = template["tests"][i]

            applicationLicenses = None
            try:
                applicationLicenses = jobSettings["applicationLicense"]
            except KeyError as e:
                pass

            _job_managers.append(
                JobManager.JobManager(
                    jobSettings["template"],
                    jobSettings["poolTemplate"],
                    jobSettings["parameters"],
                    jobSettings["expectedOutput"],
                    applicationLicenses))

        images_refernces = []
        for i in range(0, len(template["images"])):
            image = template["images"][i]
            images_refernces.append(
                Utils.ImageReference(
                    image["osType"],
                    image["offer"],
                    image["version"]))

        Utils.logger.info("{} jobs will be created".format(len(_job_managers)))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*[j.upload_assets(blob_client) for j in _job_managers]))
        Utils.logger.info("Creating pools...")
        loop.run_until_complete(asyncio.gather(*[j.create_pool(batch_client, images_refernces) for j in _job_managers]))
        Utils.logger.info("Submitting jobs...")
        loop.run_until_complete(asyncio.gather(*[j.create_and_submit_Job(batch_client) for j in _job_managers]))
        Utils.logger.info("waiting for jobs to complete...")
        loop.run_until_complete(asyncio.gather(*[j.wait_for_tasks_to_complete(batch_client, _timeout) for j in _job_managers]))

    except batchmodels.batch_error.BatchErrorException as err:
        traceback.print_exc()
        Utils.print_batch_exception(err)
        raise
    finally:
        loop = asyncio.get_event_loop()
        # Delete all the jobs and containers needed for the job
        # Reties any jobs that failed
        Utils.logger.info("-----------------------------------------")
        loop.run_until_complete(asyncio.gather(*[j.retry(batch_client, blob_client, _timeout/2) for j in _job_managers]))
        #loop.run_until_complete(asyncio.gather(*[j.delete_resouces(batch_client, blob_client) for j in _job_managers]))
        #loop.run_until_complete(asyncio.gather(*[j.delete_pool(batch_client) for j in _job_managers]))
        loop.close()
        Utils.print_result(_job_managers)

        end_time = datetime.datetime.now().replace(microsecond=0)
        Utils.export_result(_job_managers, (end_time - start_time))
    Utils.logger.info("-----------------------------------------")
    Utils.logger.info('Sample end: {}'.format(end_time))
    Utils.logger.info('Elapsed time: {}'.format(end_time - start_time))
