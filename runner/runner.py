from __future__ import print_function
from azure.common.credentials import ServicePrincipalCredentials
import traceback
import datetime
import sys
import logger
import json
import job_manager
import utils
import azure.storage.blob as azureblob
import azure.batch.models as batchmodels
import azext.batch as batch
import argparse

"""
This python module is used for validating the rendering templates by using the azure CLI. 
This module will load the manifest file 'TestConfiguration' specified by the user and
create pools and jobs based on this file.
"""

sys.path.append('.')
sys.path.append('..')

_timeout = 25  # type: int
_job_managers = []  # type: List[job_manager.JobManager]


def create_batch_client(args: object) -> batch.BatchExtensionsClient:
    """
    Create a batch client using AAD.

    :param args: The list of arguments that come in through the command line
    :type args: ArgumentParser
    :return batch.BatchExtensionsClient: returns the valid batch extension client that used AAD.
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
    Handles the user's input and what settings are needed when running this module.

    :return: Returns the parser that contains all settings this module needs from the user's input
    :rtype: Parser
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
    parser.add_argument("-VMImageURL", default=None, help="The custom image resource URL, if you want the temlates to run on a custom image")
    parser.add_argument("-VMImageType", choices=['windows','Windows','centos','Centos','CentOS', None], default=None, help="The custom image type, windows or centos")

    return parser.parse_args()


def run_job_manager_tests(blob_client: azureblob.BlockBlobService, batch_client: batch.BatchExtensionsClient,
                          images_refs: 'List[utils.ImageReference]', VMImageURL: str , VMImageType: str):
    """
    Creates all resources needed to run the job, including creating the containers and the pool needed to run the job.
    Then creates job and checks if the expected output is correct.

    :param images_refs: The list of images the rendering image will run on
    :type images_refs: List[utils.ImageReference]
    :param blob_client: The blob client needed for making blob client operations
    :type blob_client: `azure.storage.blob.BlockBlobService`
    :param batch_client: The batch client needed for making batch operations
    :type batch_client: azure.batch.BatchExtensionsClient
    """

    logger.info("{} jobs will be created.".format(len(_job_managers)))
    utils.execute_parallel_jobmanagers("upload_assets", _job_managers, blob_client)
    logger.info("Creating pools...")
    utils.execute_parallel_jobmanagers("create_pool", _job_managers, batch_client, images_refs, VMImageURL, VMImageType)
    logger.info("Submitting jobs...")
    utils.execute_parallel_jobmanagers("create_and_submit_job", _job_managers, batch_client)
    logger.info("Waiting for jobs to complete...")
    utils.execute_parallel_jobmanagers("wait_for_job_results", _job_managers, batch_client, _timeout)


def main():
    args = runner_arguments()
    logger.account_info(args)
    start_time = datetime.datetime.now().replace(microsecond=0)
    logger.info('Template runner start time: [{}]'.format(start_time))

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    blob_client = azureblob.BlockBlobService(
        account_name=args.StorageAccountName,
        account_key=args.StorageAccountKey)

    # Create a batch account using AAD    
    batch_client = create_batch_client(args)

    # Clean up any storage container that is older than a 7 days old.
    utils.cleanup_old_resources(blob_client)

    try:
        images_refs = []  # type: List[utils.ImageReference]
        with open(args.TestConfig) as f:
            template = json.load(f)

            for jobSetting in template["tests"]:
                application_licenses = None
                if 'applicationLicense' in jobSetting:
                    application_licenses = jobSetting["applicationLicense"]

                _job_managers.append(job_manager.JobManager(
                    jobSetting["template"],
                    jobSetting["poolTemplate"],
                    jobSetting["parameters"],
                    jobSetting["expectedOutput"],
                    application_licenses))

            for image in template["images"]:
                images_refs.append(utils.ImageReference(image["osType"], image["offer"], image["version"]))

        run_job_manager_tests(blob_client, batch_client, images_refs, args.VMImageURL, args.VMImageType)

    except batchmodels.batch_error.BatchErrorException as err:
        traceback.print_exc()
        utils.print_batch_exception(err)
        raise
    finally:
        # Delete all the jobs and containers needed for the job
        # Reties any jobs that failed
        utils.execute_parallel_jobmanagers("retry", _job_managers, batch_client, blob_client, _timeout / 2)
        utils.execute_parallel_jobmanagers("delete_resources", _job_managers, batch_client, blob_client)
        utils.execute_parallel_jobmanagers("delete_pool", _job_managers, batch_client)
        end_time = datetime.datetime.now().replace(microsecond=0)
        logger.print_result(_job_managers)
        logger.export_result(_job_managers, (end_time - start_time))
    logger.info('Sample end: {}'.format(end_time))
    logger.info('Elapsed time: {}'.format(end_time - start_time))


if __name__ == '__main__':
    main()
    exit(0)
