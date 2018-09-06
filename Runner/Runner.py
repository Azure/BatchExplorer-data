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
import JobTypes
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
_JOB_ID = _time
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

def submit_jobs(jobs):    
    loop = asyncio.new_event_loop()
    valid_jobs = [j.Run(batch_client) for j in jobs]        
    done, pending = loop.run_until_complete(asyncio.wait(valid_jobs))               
    for future in done:
        value = future.result()
    loop.close()


def validate_jobs(jobs):
    
    loop = asyncio.get_event_loop()
    valid_jobs = [j.Validate(batch_client) for j in jobs]        
    done, pending = loop.run_until_complete(asyncio.wait(valid_jobs))               

    for future in done:
        value = future.result()
    loop.close()

def delete_jobs(jobs):

    loop = asyncio.get_event_loop()
    valid_jobs = [j.Delete(batch_client) for j in jobs]        
    done, pending = loop.run_until_complete(asyncio.wait(valid_jobs))               
    for future in done:
        value = future.result()
    loop.close()


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
    
        jobs = []
        #------------------
        #---Maya-windows---
        #------------------
        #jobs.append(JobTypes.Job(_JOB_ID + "-maya2017-default-windows","default-windows","../ncj/maya/render-default-windows/job.template.json","maya.mb"))
        #jobs[0].set_rendering_fields("../ncj/maya/render-default-windows/pool.template.json", "%MAYA_2017_EXEC%", "maya.exr.0001")

        #jobs.append(JobTypes.Job(_JOB_ID + "-maya2018-default-windows","default-windows","../ncj/maya/render-default-windows/job.template.json","maya.mb"))
        #jobs[1].set_rendering_fields("../ncj/maya/render-default-windows/pool.template.json", "%MAYA_2018_EXEC%", "maya.exr.0001")

        #jobs.append(JobTypes.Job(_JOB_ID + "-maya2017-arnold-windows","arnold-windows","../ncj/maya/render-arnold-windows/job.template.json","maya.mb"))
        #jobs[2].set_rendering_fields("../ncj/maya/render-arnold-windows/pool.template.json", "%MAYA_2017_EXEC%", "maya.exr.0001")

        #jobs.append(JobTypes.Job(_JOB_ID + "-maya2018-arnold-windows","arnold-windows","../ncj/maya/render-arnold-windows/job.template.json","maya.mb"))
        #jobs[3].set_rendering_fields("../ncj/maya/render-arnold-windows/pool.template.json", "%MAYA_2018_EXEC%", "maya.exr.0001")

        #jobs.append(JobTypes.Job(_JOB_ID + "-maya2017-vray-windows","vray-windows","../ncj/maya/render-arnold-windows/job.template.json","maya.mb"))
        #jobs[4].set_rendering_fields("../ncj/maya/render-vray-windows/pool.template.json", "%MAYA_2017_EXEC%", "maya.exr.0001", ["maya","vray"])

        #jobs.append(JobTypes.Job(_JOB_ID + "-maya2018-vray-windows","vray-windows","../ncj/maya/render-arnold-windows/job.template.json","maya.mb"))
        #jobs[5].set_rendering_fields("../ncj/maya/render-vray-windows/pool.template.json", "%MAYA_2018_EXEC%", "maya.exr.0001", ["maya","vray"])
        
        #------------------
        #---Maya-linux-----
        #------------------
        #jobs.append(JobTypes.Job(_JOB_ID + "-maya2017-default-linux", "default-linux", "../ncj/maya/render-default-linux/job.template.json", "maya.mb", True))
        #jobs[6].set_rendering_fields("../ncj/maya/render-default-linux/pool.template.json", "maya2017", "maya.exr.0001")

        #jobs.append(JobTypes.Job(_JOB_ID + "-maya2018-default-linux","default-linux","../ncj/maya/render-default-linux/job.template.json","maya.mb", True))
        #jobs[7].set_rendering_fields("../ncj/maya/render-default-linux/pool.template.json", "maya2018", "maya.exr.0001")

        #jobs.append(JobTypes.Job(_JOB_ID + "-maya2017-arnold-linux","arnold-linux","../ncj/maya/render-arnold-linux/job.template.json","maya.mb", True))
        #jobs[8].set_rendering_fields("../ncj/maya/render-arnold-linux/pool.template.json", "maya2017", "maya.exr.0001")

        #jobs.append(JobTypes.Job(_JOB_ID + "-maya2018-arnold-linux","arnold-linux","../ncj/maya/render-arnold-linux/job.template.json","maya.mb", True))
        #jobs[9].set_rendering_fields("../ncj/maya/render-arnold-linux/pool.template.json", "maya2018", "maya.exr.0001")
        
        #jobs.append(JobTypes.Job(_JOB_ID + "-maya2017-vray-linux","vray-linux","../ncj/maya/render-vray-linux/job.template.json","maya.mb", True))
        #jobs[10].set_rendering_fields("../ncj/maya/render-vray-linux/pool.template.json", "maya2017", "maya.0001.png")

        #jobs.append(JobTypes.Job(_JOB_ID + "-maya2018-vray-linux","vray-linux","../ncj/maya/render-vray-linux/job.template.json","maya.mb", True))
        #jobs[11].set_rendering_fields("../ncj/maya/render-vray-linux/pool.template.json", "maya2018", "maya.0001.png",  ["maya","vray"])

        #---------------
        #---3ds-max-----
        #---------------     
#        jobs.append(JobTypes.Max3ds(_JOB_ID + "-3dsMax2018-arnold", "3dsMax-standard-windows", "../ncj/3dsmax/standard/job.template.json", "3ds Max 2018", "3dsmax-arnold.max"))
 #       jobs[-1].set_rendering_fields("../ncj/3dsmax/standard/pool.template.json", "image0001.jpg","arnold", ["3dsmax", "arnold"])

        #jobs.append(JobTypes.Max3ds(_JOB_ID + "-3dsMax2019-arnold", "3dsMax-standard-windows", "../ncj/3dsmax/standard/job.template.json", "3ds Max 2019", "3dsmax-arnold.max"))
        #jobs[-1].set_rendering_fields("../ncj/3dsmax/standard/pool.template.json", "image0001.jpg", "arnold", ["3dsmax", "arnold"])

        #jobs.append(JobTypes.Max3ds(_JOB_ID + "-3dsMax2018-vray", "3dsMax-standard-windows-vray", "../ncj/3dsmax/standard/job.template.json", "3ds Max 2018", "3dsmax-vray.max"))
        #jobs[-1].set_rendering_fields("../ncj/3dsmax/standard/pool.template.json", "image0001.jpg", "vray", ["3dsmax", "vray"])

        #jobs.append(JobTypes.Max3ds(_JOB_ID + "-3dsMax2019-vray", "3dsMax-standard-windows-vray", "../ncj/3dsmax/standard/job.template.json", "3ds Max 2019", "3dsmax-vray.max"))
        #jobs[-1].set_rendering_fields("../ncj/3dsmax/standard/pool.template.json", "image0001.jpg", "vray", ["3dsmax", "vray"])

        #jobs.append(JobTypes.Max3ds(_JOB_ID + "-3dsMax2018-vray-VRayRT", "3dsMax-standard-windows-vray-1", "../ncj/3dsmax/vray-dr/job.template.json", "3ds Max 2018", "3dsmax-vray.max"))
        #jobs[-1].set_rendering_fields("../ncj/3dsmax/vray-dr/pool.template.json", "image0001.jpg", "vray", ["3dsmax", "vray"], "VRayRT")        

        #jobs.append(JobTypes.Max3ds(_JOB_ID + "-3dsMax2018-vray-VRayAdv", "3dsMax-standard-windows-vray-2", "../ncj/3dsmax/vray-dr/job.template.json", "3ds Max 2018", "3dsmax-vray.max"))
        #jobs[-1].set_rendering_fields("../ncj/3dsmax/vray-dr/pool.template.json", "image0001.jpg", "vray", ["3dsmax", "vray"], "VRayAdv")        

        #jobs.append(JobTypes.Max3ds(_JOB_ID + "-3dsMax2019-vray-VRayRT", "3dsMax-standard-windows-vray-1", "../ncj/3dsmax/vray-dr/job.template.json", "3ds Max 2019", "3dsmax-vray.max"))
        #jobs[-1].set_rendering_fields("../ncj/3dsmax/vray-dr/pool.template.json", "image0001.jpg", "vray", ["3dsmax", "vray"], "VRayRT")        

        #jobs.append(JobTypes.Max3ds(_JOB_ID + "-3dsMax2019-vray-VRayAdv", "3dsMax-standard-windows-vray-2", "../ncj/3dsmax/vray-dr/job.template.json", "3ds Max 2019", "3dsmax-vray.max"))
        #jobs[-1].set_rendering_fields("../ncj/3dsmax/vray-dr/pool.template.json", "image0001.jpg", "vray", ["3dsmax", "vray"], "VRayAdv")        

        #-------------
        #---Blender---
        #-------------
        #jobs.append(JobTypes.BlenderJob(_JOB_ID + "-blender-windows","blender-linux","../ncj/blender/render-linux/job.template.json", "shapes.blend", True))
        #jobs[-1].set_rendering_fields("../ncj/blender/render-linux/pool.template.json", "", _time+"-blender-linux_0001.png")        

        #jobs.append(JobTypes.BlenderTileJob(_JOB_ID + "-blender-windows-dr","blender-linux-dr-1","../ncj/blender/render-linux-dr/job.template.json", "shapes.blend"))
        #jobs[-1].set_rendering_fields("../ncj/blender/render-linux-dr/pool.template.json", "", _time+"-blender-windows_0001.png")        

        #jobs.append(JobTypes.BlenderJob(_JOB_ID + "-blender-windows","blender-windows","../ncj/blender/render-windows/job.template.json", "shapes.blend"))
        #jobs[-1].set_rendering_fields("../ncj/blender/render-windows/pool.template.json", "", _time+"-blender-windows_0001.png")        

        #jobs.append(JobTypes.BlenderJob(_JOB_ID + "-blender-windows","blender-windows-dr","../ncj/blender/render-windows-dr/job.template.json", "shapes.blend"))
        #jobs[-1].set_rendering_fields("../ncj/blender/render-windows-dr/pool.template.json", "", _time+"-blender-windows-dr_0001.png")        

        #Vray jobs
        #doesn't work yet
        #create_blender_job(batch_client, _JOB_ID + "blender-render-windows-dr", "blender-windows-cycles-gpu", "../ncj/blender/render-windows-dr/job.template.json", "shapes.blend")

        #create_vray_job(batch_client, _JOB_ID + "linux-with-blobfuse-mount", "linux-with-blobfuse-mount", "../ncj/vray/render-linux-with-blobfuse-mount/job.template.json", "vray.vrscene")

        #jobs.append(JobTypes.ArnoldJob(_JOB_ID + "-arnold", "arnold-standalone", "../ncj/arnold/render-windows/job.template.json", "arnold.ass"))
        #jobs[0].set_rendering_fields("../ncj/3dsmax/standard/pool.template.json", "arnold.ass.tif", "")

        #jobs.append(JobTypes.VrayStandAloneJob(_JOB_ID + "-vray-standalone", "vray-standalone-pool", "../ncj/vray/render-windows/job.template.json", "vray.vrscene"))
        #jobs[-1].set_rendering_fields("../ncj/vray/render-windows/pool.template.json", "image1.png", "")

        #jobs.append(JobTypes.ArnoldStandAloneJob(_JOB_ID + "-arnold-standalone", "arnold-standalone", "../ncj/arnold/render-windows/job.template.json", "arnold.ass"))
        #jobs[-1].set_rendering_fields("../ncj/arnold/render-windows/pool.template.json", "arnold.ass.tif","arnold", ["arnold"])

        #jobs.append(JobTypes.ArnoldStandAloneJob(_JOB_ID + "-arnold-standalone-frame", "arnold-standalone-frame", "../ncj/arnold/render-windows-frames/job.template.json", "arnold.ass"))
        #jobs[-1].set_rendering_fields("../ncj/arnold/render-windows-frames/pool.template.json", "arnold.ass.tif","arnold", ["arnold"])

        jobs.append(JobTypes.ImageMagickJob(_JOB_ID + "-imagemagick", "imagemagick-pool", "../ncj/imagemagick/resize-images/job.template.json", "arnold.ass"))
        jobs[-1].set_rendering_fields("../ncj/imagemagick/resize-images/pool.template.json", "arnold.ass.tif","arnold")

        # Add the tasks to the job. 
        #loop = asyncio.get_event_loop()
        #results = loop.run_until_complete(asyncio.gather(
            #validate_job(batch_client, _JOB_ID + "-maya2017-arnold-windows","maya.exr.0002"),
            #validate_job(batch_client, _JOB_ID + "-maya2018-arnold-windows","maya.exr.0001"),
            #validate_job(batch_client, _JOB_ID + "-maya2017-vray-windows","maya-vray.0001.png"),
            #validate_job(batch_client, _JOB_ID + "-maya2017-vray-windows","maya-vray.0001.png"),
        #))
        #loop.close()

        print("Submitting {} pools ".format(len(jobs)))
        for j in jobs:
            j.create_pool(batch_client)


        print("Submitting {} jobs ".format(len(jobs)))
        submit_jobs(jobs)
        print("checking to see if all {} jobs are valid ".format(len(jobs)))
        #validate_jobs(jobs)
        print("Cleaning up the pools and jobs")
        #delete_jobs(jobs)
        
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
