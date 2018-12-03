import azure.batch.models as batchmodels
import azure.storage.blob as azureblob
from azure.storage.blob.models import ContainerPermissions
import azext.batch as batch
import datetime
import time
import os
from enum import Enum
import pytz
import logger
import threading

utc = pytz.utc

"""
Utility module that holds the data objects and some useful methods
"""


class StorageInfo(object):
    """Data object to store the StorageInfo for the job's input and output containers"""

    def __init__(self, input_container, output_container, input_container_SAS, output_container_SAS):
        super(StorageInfo, self).__init__()
        self.input_container = input_container  # The attribute 'input_container' of type 'str'
        self.output_container = output_container  # The attribute 'output_container' of type 'str'
        self.input_container_SAS = input_container_SAS  # The attribute 'input_container_SAS' of type 'str'
        self.output_container_SAS = output_container_SAS  # The attribute 'output_container_SAS' of type 'str'

    def __str__(self) -> str:
        return "[input_container: {}, output_container:{}".format(
            self.input_container, self.output_container)


class ImageReference(object):
    """Data object for holding the imageReference data"""

    def __init__(self, osType, offer, version):
        super(ImageReference, self).__init__()
        self.osType = osType  # The attribute 'osType' of type 'str'
        self.offer = offer  # The attribute 'offer' of type 'str'
        self.version = version  # The attribute 'version' of type 'str'

    def __str__(self) -> str:
        return "osType: {}, offer{}, version".format(
            self.osType, self.offer, self.version)


class JobStatus(object):
    """The Job state and the error message"""

    def __init__(self, job_state, message):
        super(JobStatus, self).__init__()
        self.job_state = job_state  # The attribute 'job_state' of type 'utils.JobState'
        self.message = message  # The attribute 'version' of type 'str'

    def __str__(self) -> str:
        return "Job's state: {}, message{}".format(
            self.job_state, self.message)


class JobState(Enum):
    """
    All the possible states of the job.
    """
    # Job never started.
    NOT_STARTED = 1
    # Pool never started due to an resize error.
    POOL_FAILED = 2
    # Job ran to completion and the output matched the test configuration file.
    COMPLETE = 3
    # The output file did not match the expected output described in the test
    # configuration file.
    UNEXPECTED_OUTPUT = 4
    # pool started but the job failed to complete in time.
    NOT_COMPLETE = 5


def print_batch_exception(batch_exception: batchmodels.batch_error.BatchErrorException):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception: The exception to convert into something readable
    :type batch_exception: batchmodels.batch_error.BatchErrorException
    """
    logger.error('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        logger.error(batch_exception.error.message.value)
        if batch_exception.error.values:
            logger.error()
            for mesg in batch_exception.error.values:
                logger.error('{}:\t{}'.format(mesg.key, mesg.value))
                logger.error('{}'.format(mesg.value))


def expected_exception(batch_exception: batchmodels.batch_error.BatchErrorException, message: str) -> bool:
    """
    If the expected exception is hit we want to return True, this is to ignore the errors
    we do not care about.

    :param batch_exception: The batch error that we want to check
    :type batch_exception: batchmodels.batch_error.BatchErrorException
    :param message: expected message that we are expecting.
    :type message: str
    :return: If the expected exception is hit return a True.
    :rtype: bool
    """
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        if message in batch_exception.error.message.value:
            return True

    return False


def get_container_sas_token(block_blob_client: azureblob.BlockBlobService,
                            container_name: str, blob_permissions: ContainerPermissions) -> str:
    """
    Obtains a shared access signature granting the specified permissions to the
    container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param container_name: The name of the Azure Blob storage container.
    :type container_name: str
    :param BlobPermissions blob_permissions:
    :type blob_permissions: azure.storage.blob.models.ContainerPermissions
    :return: A SAS token granting the specified permissions to the container.
    :rtype: str
    """
    # Obtain the SAS token for the container, setting the expiry time and
    # permissions. In this case, no start time is specified, so the shared
    # access signature becomes valid immediately.

    container_sas_token = \
        block_blob_client.generate_container_shared_access_signature(
            container_name,
            permission=blob_permissions,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

    return container_sas_token


def upload_file_to_container(block_blob_client: azureblob.BlockBlobService, container_name: str, file_path: str):
    """
    Uploads a local file to an Azure Blob storage container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param container_name: The name of the Azure Blob storage container.
    :type container_name: str
    :param str file_path: The local path to the file.
    :type file_path: str
    """
    blob_name = os.path.basename(file_path)

    logger.info(
        'Uploading file [{}] to container [{}]...'.format(
            file_path,
            container_name))

    block_blob_client.create_blob_from_path(container_name,
                                            blob_name,
                                            file_path)


def wait_for_tasks_to_complete(
        batch_service_client: batch.BatchExtensionsClient, job_id: str, timeout: datetime.timedelta) -> JobStatus:
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `batch.BatchExtensionsClient`
    :param job_id: The id of the job whose tasks should be to monitored.
    :type job_id: str
    :param timeout: The duration to wait for task completion. If all
    :type timeout: datetime.timedelta
    tasks in the specified job do not reach Completed state within this time
    period, an error message will be recorded.
    :return: The job status, with a message saying the state 
    :rtype: 'util.JobStatus'
    """
    # How long we should be checking to see if the job is complete.
    timeout_expiration = datetime.datetime.now() + timeout

    # Wait for task to complete for as long as the timeout
    while datetime.datetime.now() < timeout_expiration:

        # Grab all the tasks in the Job.
        tasks = batch_service_client.task.list(job_id)

        # Check to see how many tasks are incomplete.
        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]

        # if the all the tasks are complete we return a complete message, else
        # we wait all the tasks are complete
        if not incomplete_tasks:
            return JobStatus(JobState.COMPLETE,
                             "Job {} successfully completed.".format(job_id))
        else:
            logger.info("Job [{}] is running".format(job_id))
            time.sleep(10)

    return JobStatus(JobState.NOT_COMPLETE,
                     "ERROR: Tasks did not reach 'Completed' state within timeout period of: " + str(timeout))


def check_task_output(batch_service_client: batch.BatchExtensionsClient, job_id: str, expected_file_output_name: str) -> JobStatus:
    """Prints the stdout.txt file for each task in the job.

    :param batch_service_client: The batch client to use.
    :type batch_service_client: `Azure.Batch.BatchExtensionsClient`
    :param job_id: The id of the job with task output files to print.
    :type job_id: str
    :param expected_file_output_name: The file name of the expected output
    :type expected_file_output_name: str
    :return: The job status, with a message saying the state 
    :rtype: 'util.JobStatus'
    """

    tasks = batch_service_client.task.list(job_id)

    for task in tasks:
        all_files = batch_service_client.file.list_from_task(
            job_id, task.id, recursive=True)

        for f in all_files:
            if expected_file_output_name in f.name:
                logger.info(
                    "Job [{}] expected output matched {}".format(
                        job_id, expected_file_output_name))
                return JobStatus(JobState.COMPLETE,
                                 "File found {0}".format(expected_file_output_name))

    return JobStatus(JobState.UNEXPECTED_OUTPUT, ValueError(
        "Error: Cannot find file {} in job {}".format(expected_file_output_name, job_id)))


def cleanup_old_resources(blob_client: azureblob.BlockBlobService, days:int=7):
    """
    Delete any storage container that has been around for 7 days by default. 

    :param blob_client: A blob service client.
    :type blob_client: `azure.storage.blob.BlockBlobService`
    :param days: If the storage account is older than this number of days delete it, default = 7
    :param int: 
    """
    # The current time 7 days ago. 
    timeout = utc.localize(datetime.datetime.now()) + datetime.timedelta(days=-days)

    try:
        for container in blob_client.list_containers():
            if container.properties.last_modified < timeout:
                if 'fgrp' in container.name:
                    logger.info(
                        "Deleting container {}, it is older than 7 days.".format(container.name))
                    blob_client.delete_container(container.name)
    except Exception as e:
        logger.error(e)
        raise e


def create_thread_collection(method_name: str, job_managers: 'list[job_manager.JobManager]', *args):
    """
    Creates and runs a thread for every job_managers and runs the method_name for the given job_managers. 
    Will also wait until all the tasks are complete

    :param method_name: The job_managers method to be called
    :type method_name: str
    :param job_managers: a collection of jobs that will be run
    :type  job_managers: List[job_managers.JobManager]
    :param args: the arguments the method needs to run 
    """
    threads = []  # type: List[threading.Thread]

    for j in job_managers:
        thread = threading.Thread(target=getattr(j, method_name), args=args)
        threads.append(thread)
        thread.start()

    # wait for all threads to finish
    for thread in threads:
        thread.join()
