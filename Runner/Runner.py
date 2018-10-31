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

timeout = 15
_job_managers = []

def print_result():
        print("-----------------------------------------")
        print("Number of jobs run {}.".format(len(_job_managers)))
        failedJobs = 0
        for i in _job_managers:
            if i.job_status.job_state != Utils.JobState.COMPLETE:
                failedJobs+=1
                print("job {} failed because {} : {}".format(i.job_id, i.job_status.job_state, i.job_status.message))

        if failedJobs==0: 
            print("-----------------------------------------")
            print("All jobs were successful Run")
        else: 
            print("-----------------------------------------")
            print("Number of jobs passed {} out of {}.".format(len(_job_managers)-failedJobs, len(_job_managers)))    

if __name__ == '__main__':

    start_time = datetime.datetime.now().replace(microsecond=0)
    print('Sample start: {}'.format(start_time))
    print()
    print("_BATCH_ACCOUNT_NAME",_BATCH_ACCOUNT_NAME)
    print("_BATCH_ACCOUNT_KEY",_BATCH_ACCOUNT_KEY)
    print("_BATCH_ACCOUNT_URL",_BATCH_ACCOUNT_URL)
    print("_BATCH_ACCOUNT_SUB",_BATCH_ACCOUNT_SUB)
    print("_STORAGE_ACCOUNT_NAME",_STORAGE_ACCOUNT_NAME)
    print("_STORAGE_ACCOUNT_KEY",_STORAGE_ACCOUNT_KEY)
    print("_SERVICE_PRINCIPAL_CREDENTIALS_CLIENT_ID",_SERVICE_PRINCIPAL_CREDENTIALS_CLIENT_ID)
    print("_SERVICE_PRINCIPAL_CREDENTIALS_SECRET",_SERVICE_PRINCIPAL_CREDENTIALS_SECRET)
    print("_SERVICE_PRINCIPAL_CREDENTIALS_TENANT",_SERVICE_PRINCIPAL_CREDENTIALS_TENANT)
    print("_SERVICE_PRINCIPAL_CREDENTIALS_RESOUCE",_SERVICE_PRINCIPAL_CREDENTIALS_RESOUCE)
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
        
        for i in range(0, len(template["tests"])):  
            test = template["tests"][i]

            applicationLicenses = None
            try:
                applicationLicenses = test["applicationLicense"]
            except:
                pass
            
            _job_managers.append(JobManager.JobManager(test["template"], test["poolTemplate"], test["parameters"], test["expectedOutput"], applicationLicenses))
        
        images_refernces = []
        for i in range(0, len(template["images"])):
            image = template["images"][i]

            images_refernces.append(Utils.ImageReference(image["osType"], image["offer"], image["version"]))

        print("{} jobs will be created".format(len(_job_managers)))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*[j.upload_assets(blob_client) for j in _job_managers]))     
        print("Creating pools...")
        loop.run_until_complete(asyncio.gather(*[j.create_pool(batch_client, images_refernces) for j in _job_managers]))
        print("Submitting jobs...")
        loop.run_until_complete(asyncio.gather(*[j.create_and_submit_Job(batch_client) for j in _job_managers]))
        print("waiting for jobs to complete...")
        loop.run_until_complete(asyncio.gather(*[j.wait_for_tasks_to_complete(batch_client, timeout) for j in _job_managers]))
        
    except batchmodels.batch_error.BatchErrorException as err:
        traceback.print_exc()
        Utils.print_batch_exception(err)
        raise
    finally: 
        loop = asyncio.get_event_loop()
        # Delete all the jobs and containers needed for the job 
        # Reties any jobs that failed 
        print("-----------------------------------------")
        loop.run_until_complete(asyncio.gather(*[j.retry(batch_client, blob_client, timeout/2) for j in _job_managers]))
        loop.run_until_complete(asyncio.gather(*[j.delete_resouces(batch_client, blob_client) for j in _job_managers]))
        #loop.run_until_complete(asyncio.gather(*[j.delete_pool(batch_client) for j in _job_managers]))
        loop.close()    
        print_result()
        
    end_time = datetime.datetime.now().replace(microsecond=0)
    Utils.export_result(_job_managers, end_time-start_time)
    print()
    print('Sample end: {}'.format(end_time))
    print('Elapsed time: {}'.format(end_time - start_time))

    print()