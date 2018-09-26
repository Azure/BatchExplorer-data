import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels
import azext.batch as batch 
import json
import datetime
import time
import sys
import traceback
import asyncio
import azure.storage.blob as azureblob
import azure.batch.models.batch_error 
import os
import io
from os import listdir
from os.path import isfile, join
import asyncio

_time = str(datetime.datetime.now().hour) + "-" + str(datetime.datetime.now().minute)
#_time = "test"


def set_template_name(template, pool_id):
    try:
        template["parameters"]["poolName"]["defaultValue"] = pool_id    
    except KeyError:
        pass
    try:
        template["parameters"]["poolId"]["defaultValue"] = pool_id
    except KeyError:
        pass

def set_parameter_name(template, job_id):
    try:
        template["jobName"]["value"] = job_id    
    except KeyError:
        pass
    try:
        template["jobId"]["value"] = job_id
    except KeyError:
        pass        

def set_parameter_storage_info(template, storage_info):
    #Set input filegroup 
    try:
        template["inputData"]["value"] = storage_info.input_container        
    except KeyError:
        pass
    try:
        template["inputFilegroup"]["value"] = storage_info.input_container        
    except KeyError:
        pass  

    #Set file group SAS
    try:
        template["inputFilegroupSas"]["value"] = storage_info.input_container_SAS        
    except KeyError:
        pass
    try:
        template["inputFilegroupSas"]["value"] = storage_info.input_container_SAS        
    except KeyError:
        pass   

    #Set output filegroup
    try:
        template["outputFilegroup"]["value"] = storage_info.output_container 
    except KeyError:
        pass
    try:
        template["outputs"]["value"] = storage_info.output_container 
    except KeyError:
        pass

    try:
        template["outputSas"]["value"] = storage_info.output_container_SAS 
    except KeyError:
        pass


def set_job_template_name(template, job_id):
    try:
        template["parameters"]["jobName"]["defaultValue"] = job_id    
    except KeyError:
        pass
    try:
        template["parameters"]["jobId"]["defaultValue"] = job_id
    except KeyError:
        pass        


def get_job_id(parameters_file):
    parameters = ""
    job_id = ""
    with open(parameters_file) as f: 
        parameters = json.load(f)
    try:
        job_id = parameters["jobName"]["value"]    
    except KeyError:
        pass
    try:
        job_id = parameters["jobId"]["value"]
    except KeyError:
        pass

    return _time+"-"+job_id 

def get_pool_id(parameters_file):
    parameters = ""
    pool_id = ""

    with open(parameters_file) as f: 
        parameters = json.load(f)
    try:
        pool_id = parameters["poolName"]["value"]    
    except KeyError:
        pass
    try:
        pool_id = parameters["poolId"]["value"]
    except KeyError:
        pass

    return pool_id 

def get_scene_file(parameters_file):
    with open(parameters_file) as f: 
        parameters = json.load(f)
    try:
        sceneFile = parameters["sceneFile"]["value"]    
    except KeyError:
        pass
    try:
        sceneFile = parameters["blendFile"]["value"]
    except KeyError:
        pass

    return sceneFile


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
    print('-------------------------------------------')


def get_container_sas_token(block_blob_client,
                            container_name, blob_permissions):
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
    :rtype: `azure.batch.models.ResourceFile`
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    blob_name = os.path.basename(file_path)

    print('Uploading file {} to container [{}]...'.format(file_path,
                                                          container_name))

    block_blob_client.create_blob_from_path(container_name,
                                            blob_name,
                                            file_path)

    sas_token = block_blob_client.generate_blob_shared_access_signature(
        container_name,
        blob_name,
        permission=azureblob.BlobPermissions.READ,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

    sas_url = block_blob_client.make_blob_url(container_name,
                                              blob_name,
                                              sas_token=sas_token)

    return batchmodels.ResourceFile(file_path=blob_name,
                                    blob_source=sas_url)

async def wait_for_tasks_to_complete(batch_service_client, job_id, timeout):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be to monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.datetime.now() + timeout
    print(timeout, timeout_expiration)

    #print("Monitoring all tasks for 'Completed' state, timeout in {}...".format(timeout), end='')

    while datetime.datetime.now() < timeout_expiration:
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            return True, "job: {} successfully completed.".format(job_id)
        else:
            print("job: {} is running".format(job_id))
            await asyncio.sleep(1)

    return False, ValueError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))

async def check_task_output(batch_service_client, job_id, expected_output):
    """Prints the stdout.txt file for each task in the job.

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param str job_id: The id of the job with task output files to print.
    """
    
    #print('Printing task output...')

    tasks = batch_service_client.task.list(job_id)

    for task in tasks:    
        all_files = batch_service_client.file.list_from_task(job_id, task.id, recursive=True)
        for f in all_files:
            if expected_output in f.name:
                return True, "File found {0}".format('expected_output')

    return False, ValueError("Error: Cannot find file {} in job {}".format(expected_output, job_id))


class StorageInfo(object):
    """docstring for StorageInfo"""
    def __init__(self, input_container, output_container, input_container_SAS, output_container_SAS):
        super(StorageInfo, self).__init__()
        self.input_container = input_container
        self.output_container = output_container
        self.input_container_SAS = input_container_SAS     
        self.output_container_SAS = output_container_SAS

    def __str__(self):
        return "input_container: {} output_container:{} ".format(self.input_container, self.output_container)
