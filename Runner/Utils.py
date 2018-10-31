import azure.storage.blob as azureblob
import azure.batch.models as batchmodels
import json
import datetime
import time
import os
from enum import Enum
from xml.etree.ElementTree import Element, SubElement, tostring, ElementTree

class StorageInfo(object):
    """docstring for StorageInfo"""
    def __init__(self, input_container, output_container, input_container_SAS, output_container_SAS):
        super(StorageInfo, self).__init__()
        self.input_container = input_container
        self.output_container = output_container
        self.input_container_SAS = input_container_SAS     
        self.output_container_SAS = output_container_SAS

    def __str__(self) -> str:
        return "[input_container: {}, output_container:{}".format(self.input_container, self.output_container)
        
class JobStatus(object):
    """docstring for JobState"""
    def __init__(self, job_state, message):
        super(JobStatus, self).__init__()
        self.job_state = job_state
        self.message = message
        
    def __str__(self) -> str:
       return "job's state: {}, message{}".format(self.job_state, self.message)

class ImageReference(object):   
    """docstring for imageReferences"""
    def __init__(self, osType, offer, version):        
        super(ImageReference, self).__init__()
        self.osType = osType
        self.offer = offer
        self.version = version        

    def __str__(self) -> str:
       return "osType: {}, offer{}, version".format(self.osType, self.offer, self.version)

class JobState(Enum):
    # Job never started
    NOT_STARTED = 1
    # Pool never started due to an resize error 
    POOL_FAILED = 2
    # Job ran to completion and the output matched the test configuration file
    COMPLETE = 3
    # The outout file did not match the expected output desscibed in the test configuration file
    UNEXPECTED_OUTPUT = 4
    # pool started but the job failed to complete in time 
    NOT_COMPLETE = 5
    


def set_template_name(template, pool_id):
    try:
        template["parameters"]["poolName"]["defaultValue"] = pool_id    
    except KeyError:
        pass
    try:
        template["parameters"]["poolId"]["defaultValue"] = pool_id
    except KeyError:
        pass  
    try:
        template["parameters"]["poolId"]["value"] = pool_id
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

    """
    'fgrp-' needs to be removed.  
    """
    try:
        template["inputData"]["value"] = storage_info.input_container.replace("fgrp-", "")        
    except KeyError:
        pass
    try:
        template["inputFilegroup"]["value"] = storage_info.input_container.replace("fgrp-", "")        
    except KeyError:
        pass  

    #Set file group SAS input
    try:
        template["inputFilegroupSas"]["value"] = storage_info.input_container_SAS        
    except KeyError:
        pass
    try:
        template["inputDataSas"]["value"] = storage_info.input_container_SAS        
    except KeyError:
        pass

    #Set output filegroup
    try:
        template["outputFilegroup"]["value"] = storage_info.output_container.replace("fgrp-", "")
    except KeyError:
        pass
    try:
        template["outputs"]["value"] = storage_info.output_container.replace("fgrp-", "")
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

def set_image_reference_properties(template, image_reference):
    try:
        template["version"] = image_reference.version    
    except KeyError:
        pass
    
    try:
        template["offer"] = image_reference.offer
    except KeyError:
        pass


def set_image_reference(template, image_references):
    template_image_reference = template["variables"]["osType"]["imageReference"]
    
    # If the image is not a rendering image then no action needs to happen on the pool template 
    if template_image_reference["publisher"] != "batch": 
        return

    # If template is windows version 
    if "windows" in template_image_reference["offer"]:
        for i in range(0, len(image_references)):  
            if image_references[i].osType == "windows":
                set_image_reference_properties(template_image_reference, image_references[i])

    # if the template is centos
    if "centos" in template_image_reference["offer"]:
        for i in range(0, len(image_references)):  
            if image_references[i].osType == "liunx":
                set_image_reference_properties(template_image_reference, image_references[i])
    
def get_job_id(parameters_file: str) -> str: 
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

    return job_id 

def get_pool_id(parameters_file: str) -> str: 
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

def get_scene_file(parameters_file: str) -> str: 
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

def load_file(template_file: str) -> str: 
	template = ""
	with open(template_file) as f: 
		template = json.load(f)

	return template

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
    tasks.
    """
    
    try:    
        blob_name = os.path.basename(file_path)

        print('Uploading file [{}] to container [{}]...'.format(file_path, container_name))

        block_blob_client.create_blob_from_path(container_name,
                                            blob_name,
                                            file_path)
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise


def wait_for_tasks_to_complete(batch_service_client, job_id, timeout) -> JobStatus:
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
        #tasks = yield loop.run_in_executor(None, functools.partial(batch_service_client.task.list, batch_service_client, job_id))

        # Check to see how many tasks are incomplete. 
        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        # if the all the tasks are complete we return a complete message, else we wait all the tasks are complete 
        if not incomplete_tasks:
            return JobStatus(JobState.COMPLETE, "Job {} successfully completed.".format(job_id))
        else:
            print("Job [{}] is running".format(job_id))
            time.sleep(10)
    
    return JobStatus(JobState.NOT_COMPLETE, "ERROR: Tasks did not reach 'Completed' state within timeout period of " + str(timeout))

def check_task_output(batch_service_client, job_id, expected_output):
    """Prints the stdout.txt file for each task in the job.

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param str job_id: The id of the job with task output files to print.
    """

    tasks = batch_service_client.task.list(job_id) 
    #tasks = await loop.run_in_executor(None, functools.partial(batch_service_client.task.list, batch_service_client, job_id))
    
    for task in tasks:    
        all_files = batch_service_client.file.list_from_task(job_id, task.id, recursive=True)

        for f in all_files:
            if expected_output in f.name:
                print("Job [{}] expected output matched {}".format(job_id, expected_output))
                return JobStatus(JobState.COMPLETE, "File found {0}".format(expected_output))

    return JobStatus(JobState.UNEXPECTED_OUTPUT, ValueError("Error: Cannot find file {} in job {}".format(expected_output, job_id)))

def export_result(job_managers, time_taken):
    failedJobs = 0
    print("Exporting test output file")
    root = Element('testsuite')    
    
    for i in job_managers:
        child = SubElement(root, "testcase")
        # add a message to the error 
        child.attrib["name"] = str(i.raw_job_id)
        if i.job_status.job_state != JobState.COMPLETE:
            failedJobs+=1
            subChild = SubElement(child, "failure")
            subChild.attrib["message"] = str("Job [{}] failed due the ERROR: [{}]".format(i.job_id, i.job_status.job_state))
            subChild.text = str(i.job_status.message)

        # Add the time it took for this test to compete.
        if i.duration != None:
            test_end_time = (datetime.datetime.now().replace(microsecond=0)) - i.duration
            print("time = {}".format(str(test_end_time)))
            convertedDuration = time.strptime(str(test_end_time).split(',')[0],'%H:%M:%S')
            print("convertedDuration = {}".format(str(convertedDuration)))
            child.attrib["time"] = str(datetime.timedelta(hours=convertedDuration.tm_hour, minutes=convertedDuration.tm_min, seconds=convertedDuration.tm_sec).total_seconds())
        else: 
            child.attrib["time"] = "0:00:00" 
    
    root.attrib["failures"] = str(failedJobs)
    root.attrib["tests"] = str(len(job_managers))
    root.attrib["time"] = str(time_taken)

    tree = ElementTree(root)
    tree.write("Tests/output.xml")