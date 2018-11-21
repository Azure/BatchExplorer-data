from __future__ import print_function
from azure.common.credentials import ServicePrincipalCredentials
import traceback
import datetime
import sys
import Logger
import json
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

_timeout = 25
_job_managers = []


def create_batch_client(args: object):
    """
    Create a batch client using AAD.

    :type args: ArgumentParser
    """
    credentials = ServicePrincipalCredentials(
        client_id=args.ServicePrincipalCredentialsClientID,
        secret=args.ServicePrincipalCredentialsSecret,
        tenant=args.ServicePrincipalCredentialsTenant,
        resource=args.ServicePrincipalCredentialsResouce)

    return batch.BatchExtensionsClient(
        credentials=credentials,
        batch_account=args.BatchAccountName,
        base_url=args.BatchAccountUrl,
        subscription_id=args.BatchAccountSub)


def runner_arguments():
    """
    Handles the user's input and what settings need to be set when running this module. 

    :return: Returns all the arguments the module needs. 
    """
    parser = argparse.ArgumentParser()
    # "Tests/TestConfiguration.json"
    parser.add_argument(
        "TestConfig",
        help="A manifest file that contains a list of all the jobs and pools you want to create.")
    parser.add_argument("BatchAccountName", help="The batch account name")
    parser.add_argument("BatchAccountKey", help="The batch account key")
    parser.add_argument("BatchAccountUrl", help="The batch account url")
    parser.add_argument("BatchAccountSub", help="The batch account sub ")
    parser.add_argument("StorageAccountName", help="Storage name ")
    parser.add_argument("StorageAccountKey", help="storage key")
    parser.add_argument("ServicePrincipalCredentialsClientID", help="Service Principal id")
    parser.add_argument("ServicePrincipalCredentialsSecret", help="Service Principal secret")
    parser.add_argument("ServicePrincipalCredentialsTenant", help="Service Principal tenant")
    parser.add_argument("ServicePrincipalCredentialsResouce", help="Service Principal resource")

    return parser.parse_args()


def run_job_manager_tests(blob_client, batch_client, images_ref):
    """
    Creates all resources needed to run the job, including creating the containers and pool needed. Then
    creates job and checks it expected output.
    :param images_ref: The list of images the rendering image will run on
    :type blob_client: The batch client needed for making batch operations
    :type batch_client: The blob client needed for making blob client operations
    """

    Logger.info("{} jobs will be created".format(len(_job_managers)))
    Utils.create_thread_collection("upload_assets", _job_managers, blob_client)
    Logger.info("Creating pools...")
    Utils.create_thread_collection("create_pool", _job_managers, batch_client, images_ref)
    Logger.info("Submitting jobs...")
    Utils.create_thread_collection("create_and_submit_job", _job_managers, batch_client)
    Logger.info("waiting for jobs to complete...")
    Utils.create_thread_collection("wait_for_job_results", _job_managers, batch_client, _timeout)


def main():
    args = runner_arguments()
    Logger.account_info(args)
    start_time = datetime.datetime.now().replace(microsecond=0)
    Logger.info('Template runner start time: [{}]'.format(start_time))

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    blob_client = azureblob.BlockBlobService(
        account_name=args.StorageAccountName,
        account_key=args.StorageAccountKey)

    # Create a batch account using AAD    
    batch_client = create_batch_client(args)

    # Clean up any storage container that is older than a 7 days old.
    Utils.cleanup_old_resources(blob_client)

    try:
        images_ref = []
        with open(args.TestConfig) as f:
            template = json.load(f)

            for jobSetting in template["tests"]:
                applicationLicenses = None
                if 'applicationLicense' in jobSetting:
                    applicationLicenses = jobSetting["applicationLicense"]

                _job_managers.append(JobManager.JobManager(
                                jobSetting["template"],
                                jobSetting["poolTemplate"],
                                jobSetting["parameters"],
                                jobSetting["expectedOutput"],
                                applicationLicenses))
        
        
            for image in template["images"]:
                images_ref.append(Utils.ImageReference(image["osType"], image["offer"], image["version"]))

        run_job_manager_tests(blob_client, batch_client, images_ref)

    except batchmodels.batch_error.BatchErrorException as err:
        traceback.print_exc()
        Utils.print_batch_exception(err)
        raise
    finally:
        # Delete all the jobs and containers needed for the job
        # Reties any jobs that failed
        Logger.info("-----------------------------------------")
        Utils.create_thread_collection("retry", _job_managers, batch_client, blob_client, _timeout / 2)
        Utils.create_thread_collection("delete_resouces", _job_managers, batch_client, blob_client)
        # Utils.create_thread_collection("delete_pool", _job_managers, batch_client)
        end_time = datetime.datetime.now().replace(microsecond=0)
        Logger.print_result(_job_managers)
        Logger.export_result(_job_managers, (end_time - start_time))
    Logger.info("-----------------------------------------")
    Logger.info('Sample end: {}'.format(end_time))
    Logger.info('Elapsed time: {}'.format(end_time - start_time))


if __name__ == '__main__':
    main()
