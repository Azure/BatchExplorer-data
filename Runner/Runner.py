from __future__ import print_function
from msrestazure.azure_active_directory import AdalAuthentication 
from azure.common.credentials import ServicePrincipalCredentials
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import inspect
import traceback
import azure.batch
import azure.batch.models.batch_error 
import azure.common
import logging
import websockets
import datetime
import io
import os
import sys
import time
import json
import string
import asyncio
import JobManager
import Utils
from os import listdir
from os.path import isfile, join
import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels
import azext.batch as batch 
import argparse

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

_job_managers = []

if __name__ == '__main__':

    start_time = datetime.datetime.now().replace(microsecond=0)
    print('Sample start: {}'.format(start_time))
    print()

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    
    blob_client = azureblob.BlockBlobService(
        account_name=_STORAGE_ACCOUNT_NAME,
        account_key=_STORAGE_ACCOUNT_KEY)

    credentials = ServicePrincipalCredentials(
        client_id = _SERVICE_PRINCIPAL_CREDENTIALS_CLIENT_ID,
        secret = _SERVICE_PRINCIPAL_CREDENTIALS_SECRET,
        tenant = _SERVICE_PRINCIPAL_CREDENTIALS_TENANT,
        resource = _SERVICE_PRINCIPAL_CREDENTIALS_RESOUCE)

    batch_client = batch.BatchExtensionsClient(
        credentials=credentials,
        batch_account=_BATCH_ACCOUNT_NAME,
        base_url=_BATCH_ACCOUNT_URL,
        subscription_id=_BATCH_ACCOUNT_SUB)     

    parser = argparse.ArgumentParser()
    parser.add_argument("TestConfig", help="A manifest file that contains a list of all the jobs and pools you want to create.")
    args = parser.parse_args()
    
    TestConfigurationFile = args.TestConfig #"Tests/TestConfiguration.json"

    try:
        with open(TestConfigurationFile) as f: 
            template = json.load(f)
        
        #for i in range(0, len(template["tests"])):  # just testing out 2 files for testing. 
        for i in range(0, 2):  
            test = template["tests"][i]
            applicationLicenses = None

            try:
                applicationLicenses = test["applicationLicense"]
            except KeyError:
                pass
            
            _job_managers.append(JobManager.JobManager(test["template"], test["poolTemplate"], test["parameters"], test["expectedOutput"], applicationLicenses))


        print("Submitting {} pools ".format(len(_job_managers)))
        
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*[j.upload_assets(blob_client) for j in _job_managers]))     
        loop.run_until_complete(asyncio.gather(*[j.create_pool(batch_client) for j in _job_managers]))
        loop.run_until_complete(asyncio.gather(*[j.RunJob(batch_client) for j in _job_managers]))
        loop.run_until_complete(asyncio.gather(*[j.validate(batch_client) for j in _job_managers]))
        
    except batchmodels.batch_error.BatchErrorException as err:
        traceback.print_exc()
        Utils.print_batch_exception(err)
        raise
    finally: 
        loop = asyncio.get_event_loop()
        # Delete all the jobs and containers needed for the job 
        loop.run_until_complete(asyncio.gather(*[j.delete(batch_client, blob_client) for j in _job_managers]))
        loop.close()       

    end_time = datetime.datetime.now().replace(microsecond=0)
    print()
    print('Sample end: {}'.format(end_time))
    print('Elapsed time: {}'.format(end_time - start_time))

    print()
    input('Press ENTER to exit...')
