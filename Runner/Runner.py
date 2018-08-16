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
try:
    input = raw_input
except NameError:
    pass

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels
import azext.batch as batch 



sys.path.append('.')
sys.path.append('..')


# Update the Batch and Storage account credential strings below with the values
# unique to your accounts. These are used when constructing connection strings
# for the Batch and Storage client objects.

# global
_BATCH_ACCOUNT_NAME ='mayatest'
_BATCH_ACCOUNT_KEY = '0llHSx++QlgWzoOPSS3lwqvCvm/PtdDtKrfhSjm48CHVxBDwOjH1xmF5TZhHHukWgC9D5XReFvknk7VliGtWYQ=='
_BATCH_ACCOUNT_URL = 'https://mayatest.westcentralus.batch.azure.com'
_BATCH_ACCOUNT_SUB ='603663e9-700c-46de-9d41-e080ff1d461e'
_STORAGE_ACCOUNT_NAME = 'mayademoblob'
_STORAGE_ACCOUNT_KEY = 'fkMSiGuPJidSKLxhKfRehiCCbyA/q7QvsdZIXbx7LMmiRSIHp16l/8QpeGvaezsxvtdYFC1TZTddebmLWmgHjw=='
_POOL_ID = 'PythonQuickstartPool'
_POOL_NODE_COUNT = 2
_POOL_VM_SIZE = 'STANDARD_D2_v2'
_time = str(datetime.datetime.now().hour) + "-" + str(datetime.datetime.now().minute)
_JOB_ID = "_time"
_STANDARD_OUT_FILE_NAME = 'stdout.txt'


def query_yes_no(question, default="yes"):
    """
    Prompts the user for yes/no input, displaying the specified question text.

    :param str question: The text of the prompt for input.
    :param str default: The default if the user hits <ENTER>. Acceptable values
    are 'yes', 'no', and None.
    :rtype: str
    :return: 'yes' or 'no'
    """
    valid = {'y': 'yes', 'n': 'no'}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError("Invalid default answer: '{}'".format(default))

    while 1:
        choice = input(question + prompt).lower()
        if default and not choice:
            return default
        try:
            return valid[choice[0]]
        except (KeyError, IndexError):
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


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
        blob_name
,        permission=azureblob.BlobPermissions.READ,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

    sas_url = block_blob_client.make_blob_url(container_name,
                                              blob_name,
                                              sas_token=sas_token)

    return batchmodels.ResourceFile(file_path=blob_name,
                                    blob_source=sas_url)


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

def poolparameter_from_json(json_data): 
    """Create an ExtendedPoolParameter object from a JSON specification. 
    :param dict json_data: The JSON specification of an AddPoolParameter or an 
    ExtendedPoolParameter or a PoolTemplate. 
    """ 
    result = 'PoolTemplate' if json_data.get('properties') else 'ExtendedPoolParameter' 
    try: 
        if result == 'PoolTemplate': 
            pool = models.PoolTemplate.from_dict(json_data) 
        else: 
            pool = models.ExtendedPoolParameter.from_dict(json_data) 
        if pool is None: 
            raise ValueError("JSON data is not in correct format.") 
        return pool 
    except Exception as exp: 
        raise ValueError("Unable to deserialize to {}: {}".format(result, exp)) 


def set_template_name(template, pool_id):
    try:
        template["parameters"]["poolName"]["defaultValue"] = pool_id    
    except KeyError:
        pass
    try:
        template["parameters"]["poolId"]["defaultValue"] = pool_id
    except KeyError:
        pass


def create_pool(batch_service_client, pool_id, template_file, extra_license=""):
    """
    Creates a pool of compute nodes with the specified OS settings.
    """
    print('Creating pool [{}]...'.format(pool_id))
    
    template = ""
    with open(template_file) as f: 
        template = json.load(f)
    
    set_template_name(template, pool_id)
    
    if(extra_license!=""):
        template["pool"]["applicationLicenses"] = extra_license

    all_pools = [p.id for p in batch_service_client.pool.list()]
    if(pool_id not in all_pools):
        pool_json = batch_service_client.pool.expand_template(template)
        pool = batch_service_client.pool.poolparameter_from_json(pool_json)
        batch_service_client.pool.add(pool)
    else:
        print('pool [{}] already exists'.format(pool_id))

def submit_job(batch_service_client, template):
    """
    Submit job
    """
    job_json = batch_service_client.job.expand_template(template)
    job = batch_service_client.job.jobparameter_from_json(job_json)
    batch_service_client.job.add(job)


def update_template_OutFiles(template_node_outfiles, job_id):
    """
    Adds the prefix ID of the job_id to file group and path. 
    """
    for i in range(0, len(template_node_outfiles)):
        autoStorage = template_node_outfiles[i]["destination"]["autoStorage"]
        autoStorage["fileGroup"] = "rendering-output"
        autoStorage["path"] = autoStorage["path"].replace("[parameters('jobName')]", job_id)  

def create_job(batch_service_client, job_id, pool_id, template_file, render_version, scene_file, isLinux=False):
    """
    Creates a job with the specified ID, associated with the specified pool.
    """
    print('Creating job [{}]...'.format(job_id)," job will run on [{}]".format(pool_id))

    job = batch.models.JobAddParameter(job_id, batch.models.PoolInformation(pool_id=pool_id))

    with open(template_file) as f: 
        template = json.load(f)
    
    template["parameters"]["poolId"]["defaultValue"] = pool_id
    template["parameters"]["jobName"]["defaultValue"] = job_id
    template["parameters"]["inputData"]["defaultValue"] = "rendering"
    commandLine = template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"]

    if("additionalFlags" in template):
        template["parameters"]["additionalFlags"]["defaultValue"] = "-of png"

    if(isLinux):
        newCommandLine = commandLine.replace("[parameters('mayaVersion')]", render_version).replace("[parameters('sceneFile')]", scene_file)
    else:
        newCommandLine = commandLine.replace("[variables('MayaVersions')[parameters('mayaVersion')].environmentValue]", render_version).replace("[parameters('sceneFile')]", scene_file)

    template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"] = newCommandLine
        
    update_template_OutFiles(template["job"]["properties"]["taskFactory"]["repeatTask"]["outputFiles"], job_id)
    
    submit_job(batch_service_client, template)


def create_3ds_Max_job(batch_service_client, job_id, pool_id, template_file, max_version, renderer, scene_file, vray_renderer=""):
    """
    Creates a job with the specified ID, associated with the specified pool.
    """
    print('Creating job [{}]...'.format(job_id)," job will run on [{}]".format(pool_id))

    job = batch.models.JobAddParameter(job_id, batch.models.PoolInformation(pool_id=pool_id))
    with open(template_file) as f: 
        template = json.load(f)
    
    print(template)    


    template["parameters"]["poolId"]["defaultValue"] = pool_id
    template["parameters"]["jobName"]["defaultValue"] = job_id
    template["parameters"]["inputFilegroupSas"]["defaultValue"] = "https://mayademoblob.blob.core.windows.net/fgrp-rendering?st=2018-08-13T03%3A37%3A42Z&se=2018-08-20T03%3A52%3A42Z&sp=rl&sv=2018-03-28&sr=c&sig=lpYc5NuYSmJ%2BYGcJyaedSXFe9kZXBuDWMCkAxHnXXBQ%3D"
    template["parameters"]["sceneFile"]["defaultValue"] = scene_file
    template["parameters"]["outputFilegroup"]["defaultValue"] = "rendering-output"
    
    commandLine = ""
    # Use the VRayRT or VRayADV
    if vray_renderer:
        update_template_OutFiles(template["job"]["properties"]["taskFactory"]["tasks"][0]["outputFiles"], job_id)
        commandLine = template["job"]["properties"]["taskFactory"]["tasks"][0]["commandLine"]
        newCommandLine = commandLine.replace("[parameters('maxVersion')]", max_version).replace("[parameters('renderer')]", renderer).replace("[parameters('sceneFile')]", scene_file)
        template["job"]["properties"]["taskFactory"]["tasks"][0]["commandLine"] = newCommandLine
        template["job"]["properties"]["properties"] = job_id
        template["job"]["properties"]["poolInfo"]["pool_id"] = pool_id
        coordinationCommandLine = template["job"]["properties"]["taskFactory"]["tasks"][0]["multiInstanceSettings"]["coordinationCommandLine"]
        newCoordinationCommandLine = coordinationCommandLine.replace("[parameters('vrayRenderer')]", vray_renderer).replace("[parameters('maxVersion')]", max_version)
        template["job"]["properties"]["taskFactory"]["tasks"][0]["multiInstanceSettings"]["coordinationCommandLine"] = newCoordinationCommandLine
    
    else: #Use the arnold renderer 
        update_template_OutFiles(template["job"]["properties"]["taskFactory"]["repeatTask"]["outputFiles"], job_id)
        commandLine = template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"]
        newCommandLine = commandLine.replace("[parameters('maxVersion')]", max_version).replace("[parameters('renderer')]", renderer).replace("[parameters('sceneFile')]", scene_file)
        template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"] = newCommandLine
    
    print(template)    
    submit_job(batch_service_client, template)


def create_blender_job(batch_service_client, job_id, pool_id, template_file, scene_file):
    print('Creating job [{}]...'.format(job_id)," job will run on [{}]".format(pool_id))

    job = batch.models.JobAddParameter(job_id, batch.models.PoolInformation(pool_id=pool_id))
    with open(template_file) as f: 
        template = json.load(f)
    
    template["parameters"]["poolId"]["defaultValue"] = pool_id
    template["parameters"]["jobName"]["defaultValue"] = job_id
    template["parameters"]["inputData"]["defaultValue"] = "rendering"
    template["parameters"]["blendFile"]["defaultValue"] = scene_file
    template["parameters"]["inputDataSas"]["defaultValue"] = "https://mayademoblob.blob.core.windows.net/fgrp-rendering?st=2018-08-13T03%3A37%3A42Z&se=2018-08-20T03%3A52%3A42Z&sp=rl&sv=2018-03-28&sr=c&sig=lpYc5NuYSmJ%2BYGcJyaedSXFe9kZXBuDWMCkAxHnXXBQ%3D"
        
    commandLine = template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"]
    newCommandLine = commandLine.replace("[parameters('jobName')]", job_id).replace("[parameters('blendFile')]", scene_file)
    template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"] = newCommandLine

    update_template_OutFiles(template["job"]["properties"]["taskFactory"]["repeatTask"]["outputFiles"], job_id)
    
    submit_job(batch_service_client, template)

def create_vray_job(batch_service_client, job_id, pool_id, template_file, scene_file):
    print('Creating job [{}]...'.format(job_id)," job will run on [{}]".format(pool_id))
    print()

    job = batch.models.JobAddParameter(job_id, batch.models.PoolInformation(pool_id=pool_id))
    with open(template_file) as f: 
        template = json.load(f)

    template["parameters"]["poolId"]["defaultValue"] = pool_id
    template["parameters"]["jobName"]["defaultValue"] = job_id
    template["parameters"]["inputData"]["defaultValue"] = "rendering"
    template["parameters"]["sceneFile"]["defaultValue"] = scene_file

    commandLine = template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"]
    newCommandLine = commandLine.replace("[parameters('sceneFile')]", scene_file)
    template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"] = newCommandLine

    print(template)
    update_template_OutFiles(template["job"]["properties"]["taskFactory"]["repeatTask"]["outputFiles"], job_id)    
    submit_job(batch_service_client, template)

def create_arnold_job(batch_service_client, job_id, pool_id, template_file, scene_file):
    print('Creating job [{}]...'.format(job_id)," job will run on [{}]".format(pool_id))
    print()

    job = batch.models.JobAddParameter(job_id, batch.models.PoolInformation(pool_id=pool_id))
    with open(template_file) as f: 
        template = json.load(f)

    template["parameters"]["poolId"]["defaultValue"] = pool_id
    template["parameters"]["jobName"]["defaultValue"] = job_id
    template["parameters"]["inputData"]["defaultValue"] = "rendering"
    template["parameters"]["sceneFile"]["defaultValue"] = scene_file

    update_template_OutFiles(template["job"]["properties"]["taskFactory"]["tasks"][0]["outputFiles"], job_id)
    commandLine = template["job"]["properties"]["taskFactory"]["tasks"][0]["commandLine"]
    newCommandLine = commandLine.replace("[parameters('sceneFile')]", scene_file)
    template["job"]["properties"]["taskFactory"]["tasks"][0]["commandLine"] = newCommandLine

    print(template)
    submit_job(batch_service_client, template)



def wait_for_tasks_to_complete(batch_service_client, job_id, timeout):
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

    print("Monitoring all tasks for 'Completed' state, timeout in {}..."
          .format(timeout), end='')

    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True
        else:
            time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))


def check_task_output(batch_service_client, job_id, expected_output):
    """Prints the stdout.txt file for each task in the job.

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param str job_id: The id of the job with task output files to print.
    """
    
    print('Printing task output...')

    tasks = batch_service_client.task.list(job_id)

    for task in tasks:    
        all_files = batch_service_client.file.list_from_task(job_id, task.id, recursive=True)
        for f in all_files:
            if expected_output in f.name:
                return True

    return [False, ValueError("cannot find file {} in job {}".format(expected_output, job_id))]

def _read_stream_as_string(stream, encoding):
    """Read stream as string

    :param stream: input stream generator
    :param str encoding: The encoding of the file. The default is utf-8.
    :return: The file content.
    :rtype: str
    """
    output = io.BytesIO()
    try:
        for data in stream:
            output.write(data)
        if encoding is None:
            encoding = 'utf-8'
        return output.getvalue().decode(encoding)
    finally:
        output.close()
    raise RuntimeError('could not write data to stream or decode bytes')

def validate_job(batch_service_client, job_id, expected_output):
    wait_for_tasks_to_complete(batch_service_client, job_id, datetime.timedelta(minutes=30))
    return check_task_output(batch_service_client, job_id, expected_output)


if __name__ == '__main__':

    start_time = datetime.datetime.now().replace(microsecond=0)
    print('Sample start: {}'.format(start_time))
    print()

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.

    
    blob_client = azureblob.BlockBlobService(
        account_name=_STORAGE_ACCOUNT_NAME,
        account_key=_STORAGE_ACCOUNT_KEY)

    # Use the blob client to create the containers in Azure Storage if they
    # don't yet exist.
 
    input_container_name = 'rendering'
    #blob_client.create_container(input_container_name, fail_on_exist=False)

    OUTPUT_container_name = 'rendering-output'
    #blob_client.create_container(OUTPUT_container_name, fail_on_exist=False)

       

    # The collection of data files that are to be processed by the tasks.
    #input_file_paths = [os.path.realpath('./maya.mb')]

    # Upload the data files. 
    #input_files = [
    #    upload_file_to_container(blob_client, input_container_name, file_path)
    #    for file_path in input_file_paths]


    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage

    credentials = ServicePrincipalCredentials(
        client_id = 'a1139b6c-543e-48bf-9bce-83954d3d18d1',
        secret = '6e2f85e60d8e80f67d9b3e14f0fc28fea9cd37b9406485',
        tenant = '72f988bf-86f1-41af-91ab-2d7cd011db47',
        resource='https://batch.core.windows.net/')

    batch_client = batch.BatchExtensionsClient(
        credentials=credentials,
        batch_account=_BATCH_ACCOUNT_NAME,
        base_url=_BATCH_ACCOUNT_URL,
        subscription_id=_BATCH_ACCOUNT_SUB)
     

    try:
        # Create the pool that will contain the compute nodes that will execute the        

        #create_pool(batch_client, "default-linux", "../ncj/maya/render-default-linux/pool.template.json")
        #create_pool(batch_client, "vray-linux", "../ncj/maya/render-vray-linux/pool.template.json",["maya","vray"])
        #create_pool(batch_client, "arnold-windows", "../ncj/maya/render-arnold-windows/pool.template.json")
        #create_pool(batch_client, "vray-windows", "../ncj/maya/render-vray-windows/pool.template.json",["maya","vray"])
        #create_pool(batch_client, "default-windows", "../ncj/maya/render-default-windows/pool.template.json")
        
        #3dsmax 
        #create_pool(batch_client, "3dsMax-standard-windows-1", "../ncj/3dsmax/standard/pool.template.json")
        #create_pool(batch_client, "3dsMax-vray-windows-1", "../ncj/3dsmax/standard/pool.template.json", ["3dsmax","vray"])
        #create_pool(batch_client, "3dsMax-vray-windows-dr1", "../ncj/3dsmax/vray-dr/pool.template.json", ["3dsmax","vray"])
        #create_pool(batch_client, "3dsMax-vray-windows-dr2", "../ncj/3dsmax/vray-dr/pool.template.json", ["3dsmax","vray"])

        #blender
        #create_pool(batch_client, "blender-linux", "../ncj/blender/render-linux/pool.template.json")
        #create_pool(batch_client, "blender-linux-dr", "../ncj/blender/render-linux-dr/pool.template.json")
        #create_pool(batch_client, "blender-windows", "../ncj/blender/render-windows/pool.template.json")
        #create_pool(batch_client, "blender-windows-cycles-gpu", "../ncj/blender/render-windows-cycles-gpu/pool.template.json")
        #create_pool(batch_client, "blender-render-windows-dr", "../ncj/blender/render-windows-dr/pool.template.json")

        #arnold
        #create_pool(batch_client, "arnold-windows", "../ncj/arnold/render-windows/pool.template.json")
        
        #vrays 
        #create_pool(batch_client, "vray-render-linux", "../ncj/vray/render-linux/pool.template.json")
        #create_pool(batch_client, "linux-with-blobfuse-mount", "../ncj/vray/render-linux-with-blobfuse-mount/pool.template.json")
        #create_pool(batch_client, "vray-render-windows", "../ncj/vray/render-windows/pool.template.json")
        
        #imagemagick 
        #create_pool(batch_client, "imagemagick-linux", "../ncj/vray/render-windows/pool.template.json")
        ##Create the job that will run the tasks.

        #create_job(batch_client, _JOB_ID + "-maya2017-default-windows", "default-windows", "../ncj/maya/render-default-windows/job.template.json", "%MAYA_2017_EXEC%", "maya.mb")
        #create_job(batch_client, _JOB_ID + "-maya2018-default-windows", "default-windows", "../ncj/maya/render-default-windows/job.template.json", "%MAYA_2018_EXEC%", "maya.mb")        
        #create_job(batch_client, _JOB_ID + "-maya2017-arnold-windows", "arnold-windows", "../ncj/maya/render-arnold-windows/job.template.json", "%MAYA_2017_EXEC%", "maya.mb")
        #create_job(batch_client, _JOB_ID + "-maya2018-arnold-windows", "arnold-windows", "../ncj/maya/render-arnold-windows/job.template.json", "%MAYA_2018_EXEC%", "maya.mb")        
        #create_job(batch_client, _JOB_ID + "-maya2017-vray-windows", "vray-windows", "../ncj/maya/render-vray-windows/job.template.json", "%MAYA_2017_EXEC%", "maya-vray.mb")
        #create_job(batch_client, _JOB_ID + "-maya2018-vray-windows", "vray-windows", "../ncj/maya/render-vray-windows/job.template.json", "%MAYA_2018_EXEC%", "VRayAdv-vray.mb")     

        #create_job(batch_client, _JOB_ID + "maya-2017-default-linux", "default-linux", "../ncj/maya/render-default-linux/job.template.json","maya2017", "maya.mb", True)
        #create_job(batch_client, _JOB_ID + "maya-2018-default-linux", "default-linux", "../ncj/maya/render-default-linux/job.template.json", "maya2018", "maya.mb")
        #create_job(batch_client, _JOB_ID + "maya-2017-arnold-linux", "arnold-linux", "../ncj/maya/render-arnold-linux/job.template.json","maya2017", "maya.mb", True)
        #create_job(batch_client, _JOB_ID + "maya-2018-arnold-linux", "arnold-linux", "../ncj/maya/render-arnold-linux/job.template.json", "maya2018", "maya.mb")
        #create_job(batch_client, _JOB_ID + "maya-2017-vray-linux", "vray-linux", "../ncj/maya/render-vray-linux/job.template.json","maya2017", "maya.mb", True)
        #create_job(batch_client, _JOB_ID + "maya-2018-vray-linux", "vray-linux", "../ncj/maya/render-vray-linux/job.template.json", "maya2018", "maya.mb")

        #create_blender_job(batch_client, _JOB_ID + "blender-linux", "blender-linux", "../ncj/blender/render-linux/job.template.json", "shapes.blend")
        #create_blender_job(batch_client, _JOB_ID + "blender-windows", "blender-windows", "../ncj/blender/render-windows/job.template.json", "shapes.blend")
        #create_blender_job(batch_client, _JOB_ID + "blender-linux-dr", "blender-linux-dr", "../ncj/blender/render-linux-dr/job.template.json", "shapes.blend")
        #create_blender_job(batch_client, _JOB_ID + "blender-render-windows-dr", "blender-windows-cycles-gpu", "../ncj/blender/render-windows-dr/job.template.json", "shapes.blend")

        #3ds Max
        #create_3ds_Max_job(batch_client,  _JOB_ID + "-3dsMax2018-arnold", "3dsMax-standard-windows-1", "../ncj/3dsmax/standard/job.template.json", "3ds Max 2018","arnold", "3dsmax-arnold.max")
        #create_3ds_Max_job(batch_client,  _JOB_ID + "-3dsMax2018-vray", "3dsMax-vray-windows-1", "../ncj/3dsmax/standard/job.template.json", "3ds Max 2018","vray", "3dsmax-vray.max")
        #create_3ds_Max_job(batch_client,  _JOB_ID + "-3dsMax2019-arnold", "3dsMax-standard-windows-1", "../ncj/3dsmax/standard/job.template.json", "3ds Max 2019","arnold", "3dsmax-arnold.max")     
        #create_3ds_Max_job(batch_client,  _JOB_ID + "-3dsMax2019-vray", "3dsMax-vray-windows-1", "../ncj/3dsmax/standard/job.template.json", "3ds Max 2019","vray", "3dsmax-vray.max")     
        #create_3ds_Max_job(batch_client,  _JOB_ID + "-3dsMax2019-vray", "3dsMax-vray-windows-1", "../ncj/3dsmax/vray-dr/job.template.json", "3ds Max 2019","vray", "3dsmax-vray.max")     
        #create_3ds_Max_job(batch_client,  _JOB_ID + "-3dsMax2018-vray-VRayRT", "3dsMax-vray-windows-dr1", "../ncj/3dsmax/vray-dr/job.template.json", "3ds Max 2018", "vray", "3dsmax-vray.max", "VRayRT")  
        #create_3ds_Max_job(batch_client,  _JOB_ID + "-3dsMax2018-vray-VRayAdv", "3dsMax-vray-windows-dr2", "../ncj/3dsmax/vray-dr/job.template.json", "3ds Max 2018", "vray", "3dsmax-vray.max", "VRayAdv")
        #create_3ds_Max_job(batch_client,  _JOB_ID + "-3dsMax2019-vray-VRayRT", "3dsMax-vray-windows-dr1", "../ncj/3dsmax/vray-dr/job.template.json", "3ds Max 2019", "vray", "3dsmax-vray.max", "VRayRT")          
        #create_3ds_Max_job(batch_client,  _JOB_ID + "-3dsMax2019-vray-VRayAdv", "3dsMax-vray-windows-dr2", "../ncj/3dsmax/vray-dr/job.template.json", "3ds Max 2019", "vray", "3dsmax-vray.max", "VRayAdv")  

        #Vray jobs
        #create_vray_job(batch_client, _JOB_ID + "vray-linux", "vray-render-linux", "../ncj/vray/render-linux/job.template.json", "vray.vrscene")
        #create_vray_job(batch_client, _JOB_ID + "linux-with-blobfuse-mount", "linux-with-blobfuse-mount", "../ncj/vray/render-linux-with-blobfuse-mount/job.template.json", "vray.vrscene")
        #create_arnold_job(batch_client, _JOB_ID + "linux-render-windows", "arnold-windows", "../ncj/arnold/render-windows/job.template.json", "arnold.ass")

        print(validate_job(batch_client, _JOB_ID + "-maya2017-default-windows","maya.exr.0001"))
        print(validate_job(batch_client, _JOB_ID + "-maya2018-default-windows","maya.exr.0001"))
        print(validate_job(batch_client, _JOB_ID + "-maya2017-arnold-windows","maya.exr.0001"))
        print(validate_job(batch_client, _JOB_ID + "-maya2017-arnold-windows","maya.exr.0001"))
        print(validate_job(batch_client, _JOB_ID + "-maya2017-vray-windows","maya.exr.0001"))
        print(validate_job(batch_client, _JOB_ID + "-maya2017-vray-windows","maya.exr.0001"))
        # Add the tasks to the job. 
        #add_tasks(batch_client, _JOB_ID, input_files)

        # Pause execution until tasks reach Completed state.
        #wait_for_tasks_to_complete(batch_client, _JOB_ID, datetime.timedelta(minutes=30))

        #print("pool was created")


        #print("  Success! All tasks reached the 'Completed' state within the "
         # "specified timeout period.")

        # Print the stdout.txt and stderr.txt files for each task to the console
        ###check_task_output(batch_client, _JOB_ID)

    except batchmodels.batch_error.BatchErrorException as err:
        traceback.print_exc()
        print_batch_exception(err)
        raise

    # Clean up storage resources
    #print('Deleting container [{}]...'.format(input_container_name))
    #blob_client.delete_container(input_container_name)

    # Print out some timing info
    end_time = datetime.datetime.now().replace(microsecond=0)
    print()
    print('Sample end: {}'.format(end_time))
    print('Elapsed time: {}'.format(end_time - start_time))
    print()

    # Clean up Batch resources (if the user so chooses).
#    if query_yes_no('Delete job?') == 'yes':
        #batch_client.job.delete(_JOB_ID)

 #   if query_yes_no('Delete pool?') == 'yes':
        #batch_client.pool.delete(_POOL_ID)

    print()
    input('Press ENTER to exit...')
