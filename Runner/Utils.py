import azure.storage.blob as azureblob
import azure.batch.models as batchmodels
import json
import datetime
import time
import os
from enum import Enum
from xml.etree.ElementTree import Element, SubElement, tostring, ElementTree
import pytz
utc = pytz.UTC

"""
Utility module that holds the data objects and some useful methods
"""

class StorageInfo(object):
    """Data objects to store the for StorageInfo for the job's input and output containers"""

    def __init__(self, input_container, output_container, input_container_SAS, output_container_SAS):
        super(StorageInfo, self).__init__()
        self.input_container = input_container
        self.output_container = output_container
        self.input_container_SAS = input_container_SAS
        self.output_container_SAS = output_container_SAS

    def __str__(self) -> str:
        return "[input_container: {}, output_container:{}".format(
            self.input_container, self.output_container)


class ImageReference(object):
    """Data object for holding the imageReference data"""

    def __init__(self, osType, offer, version):
        super(ImageReference, self).__init__()
        self.osType = osType
        self.offer = offer
        self.version = version

    def __str__(self) -> str:
        return "osType: {}, offer{}, version".format(
            self.osType, self.offer, self.version)


class JobStatus(object):
    """The Job state and the error message"""

    def __init__(self, job_state, message):
        super(JobStatus, self).__init__()
        self.job_state = job_state
        self.message = message

    def __str__(self) -> str:
        return "Job's state: {}, message{}".format(
            self.job_state, self.message)


class JobState(Enum):
    # Job never started
    NOT_STARTED = 1
    # Pool never started due to an resize error
    POOL_FAILED = 2
    # Job ran to completion and the output matched the test configuration file
    COMPLETE = 3
    # The outout file did not match the expected output desscibed in the test
    # configuration file
    UNEXPECTED_OUTPUT = 4
    # pool started but the job failed to complete in time
    NOT_COMPLETE = 5


def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
                print('{}'.format(mesg.value))
    print('-------------------------------------------')


def expected_exception(batch_exception, message) -> bool:
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        if message in batch_exception.error.message.value:
            return True

    return False


def get_container_sas_token(block_blob_client,
                            container_name, blob_permissions) -> str:
    """
    Obtains a shared access signature granting the specified permissions to the
    container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param BlobPermissions blob_permissions:
    :rtype: str
    :return: A SAS token granting the specified permissions to the container.
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


def upload_file_to_container(block_blob_client, container_name, file_path):
    """
    Uploads a local file to an Azure Blob storage container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    """
    blob_name = os.path.basename(file_path)

    print(
        'Uploading file [{}] to container [{}]...'.format(
            file_path,
            container_name))

    block_blob_client.create_blob_from_path(container_name,
                                            blob_name,
                                            file_path)


def wait_for_tasks_to_complete(
        batch_service_client, job_id, timeout) -> JobStatus:
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be to monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    # How long we should be checking to see if the job is complete.
    timeout_expiration = datetime.datetime.now() + timeout

    # Wait for task to complete for as long as the timeout
    while datetime.datetime.now() < timeout_expiration:
        #print("{}, {}".format(datetime.datetime.now(),timeout_expiration))
        # Grab all the tasks in the Job.
        tasks = batch_service_client.task.list(job_id)
        # tasks = yield loop.run_in_executor(None,
        # functools.partial(batch_service_client.task.list,
        # batch_service_client, job_id))

        # Check to see how many tasks are incomplete.
        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        # if the all the tasks are complete we return a complete message, else
        # we wait all the tasks are complete
        if not incomplete_tasks:
            return JobStatus(JobState.COMPLETE,
                             "Job {} successfully completed.".format(job_id))
        else:
            print("Job [{}] is running".format(job_id))
            time.sleep(10)

    return JobStatus(JobState.NOT_COMPLETE,
                     "ERROR: Tasks did not reach 'Completed' state within timeout period of: " + str(timeout))


def check_task_output(batch_service_client, job_id, expected_output):
    """Prints the stdout.txt file for each task in the job.

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param str job_id: The id of the job with task output files to print.
    """

    tasks = batch_service_client.task.list(job_id)

    for task in tasks:
        all_files = batch_service_client.file.list_from_task(
            job_id, task.id, recursive=True)

        for f in all_files:
            if expected_output in f.name:
                print(
                    "Job [{}] expected output matched {}".format(
                        job_id, expected_output))
                return JobStatus(JobState.COMPLETE,
                                 "File found {0}".format(expected_output))

    return JobStatus(JobState.UNEXPECTED_OUTPUT, ValueError(
        "Error: Cannot find file {} in job {}".format(expected_output, job_id)))


def print_result(job_managers):
    print("-----------------------------------------")
    print("Number of jobs run {}.".format(len(job_managers)))
    failedJobs = 0
    for i in job_managers:
        if i.job_status.job_state != JobState.COMPLETE:
            failedJobs += 1
            print(
                "job {} failed because {} : {}".format(
                    i.job_id,
                    i.job_status.job_state,
                    i.job_status.message))

    if failedJobs == 0:
        print("-----------------------------------------")
        print("All jobs were successful Run")
    else:
        print("-----------------------------------------")
        print("Number of jobs passed {} out of {}.".format(
            len(job_managers) - failedJobs, len(job_managers)))


def export_result(job_managers, total_time):
    failedJobs = 0
    print("Exporting test output file")
    root = Element('testsuite')

    for i in job_managers:
        child = SubElement(root, "testcase")
        # Add a message to the error
        child.attrib["name"] = str(i.raw_job_id)
        if i.job_status.job_state != JobState.COMPLETE:
            failedJobs += 1
            subChild = SubElement(child, "failure")
            subChild.attrib["message"] = str("Job [{}] failed due the ERROR: [{}]".format(
                    i.job_id, i.job_status.job_state))

            subChild.text = str(i.job_status.message)

        # Add the time it took for this test to compete.
        if i.duration is not None:
            test_end_time = i.duration
            convertedDuration = time.strptime(str(test_end_time).split(',')[0], '%H:%M:%S.%f')
            child.attrib["time"] = str(
                datetime.timedelta(
                    hours=convertedDuration.tm_hour,
                    minutes=convertedDuration.tm_min,
                    seconds=convertedDuration.tm_sec).total_seconds())
        else:
            child.attrib["time"] = "0:00:00"

    root.attrib["failures"] = str(failedJobs)
    root.attrib["tests"] = str(len(job_managers))
    root.attrib["time"] = str(total_time.total_seconds())
    tree = ElementTree(root)
    tree.write("Tests/output.xml")


def cleanup_old_resources(blob_client):
    """
    Delete any storage container that has been around for 7 days. 
    """
    timeout = utc.localize(datetime.datetime.now()) + datetime.timedelta(days=-7)

    for container in blob_client.list_containers():
        if (container.properties.last_modified < timeout):
            if 'fgrp' in container.name:
                print(
                    "Deleting container {} that is older than 7 days.".format(container))
                blob_client.delete_container(container.name)
