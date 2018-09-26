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
import Utils
import os
from os import listdir
from os.path import isfile, join
from azure.storage.blob.models import BlobBlock, ContainerPermissions, ContentSettings
import asyncio
import datetime
import random
import types
import functools  

async def submit_job(batch_service_client, template, parameters):
    job_json = batch_service_client.job.expand_template(template, parameters)
    job = batch_service_client.job.jobparameter_from_json(job_json)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, functools.partial(batch_service_client.job.add, job))    


class JobManager(object):

    def __init__(self, template_file, pool_template_file, parameters_file, expected_output, application_licenses=None):
        super(JobManager, self).__init__()
        self.job_id = Utils.get_job_id(parameters_file)
        self.pool_id = Utils.get_pool_id(parameters_file)
        self.template_file = template_file
        self.parameters_file = parameters_file
        self.application_licenses = application_licenses
        self.expected_output = expected_output
        self.pool_template_file = pool_template_file
        self.storage_info = None 
        self.job_status = False, "Job hasn't started yet."

    
    def __str__(self):
        return "job_id: {} pool_id:{} ".format(self.job_id, self.pool_id)

    async def RunJob(self, batch_service_client):
        print('Creating job [{}]...'.format(self.job_id)," job will run on [{}]".format(self.pool_id))
        
        # load the template and parameters file 
        template = ""
        with open(self.template_file) as f: 
            template = json.load(f)
    
        parameters = ""
        with open(self.parameters_file) as f: 
            parameters = json.load(f)
    
        Utils.set_parameter_name(parameters, self.job_id)
        Utils.set_parameter_storage_info(parameters, self.storage_info)
        await submit_job(batch_service_client, template, parameters)


    async def create_pool(self, batch_service_client):
        print('Creating pool [{}]...'.format(self.pool_id))
        template = ""
        with open(self.pool_template_file) as f: 
            template = json.load(f)
        
        # set extra license if needed 
        if self.application_licenses is not None:
            template["pool"]["applicationLicenses"] = self.application_licenses.split(",")

        # List of the pools
        all_pools = [p.id for p in batch_service_client.pool.list()]
        
        Utils.set_template_name(template, self.pool_id)
        if(self.pool_id not in all_pools):
            pool_json = batch_service_client.pool.expand_template(template)
            pool = batch_service_client.pool.poolparameter_from_json(pool_json)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, functools.partial(batch_service_client.pool.add, pool))
        else:
            print('pool [{}] already exists'.format(self.pool_id))

    async def upload_assets(self, blob_client):
        print("uploading asset")
        loop = asyncio.get_event_loop()
        input_container_name = self.job_id

        # Create input container 
        await loop.run_in_executor(None, functools.partial(blob_client.create_container, "fgrp-"+input_container_name, fail_on_exist=False))    
        #blob_client.create_container("fgrp-"+input_container_name, fail_on_exist=False)
        output_container_name = self.job_id+'-output'

        # Create output container 
        #blob_client.create_container("fgrp-"+output_container_name, fail_on_exist=False)
        await loop.run_in_executor(None, functools.partial(blob_client.create_container, "fgrp-"+output_container_name, fail_on_exist=False))

        scenefile = Utils.get_scene_file(self.parameters_file)

        full_sas_url_input = 'https://{}.blob.core.windows.net/{}?{}'.format(blob_client.account_name, input_container_name, Utils.get_container_sas_token(blob_client, input_container_name, ContainerPermissions.READ + ContainerPermissions.LIST))
        full_sas_url_output = 'https://{}.blob.core.windows.net/{}?{}'.format(blob_client.account_name, output_container_name, Utils.get_container_sas_token(blob_client, output_container_name, ContainerPermissions.READ + ContainerPermissions.LIST))
        
        # Set the storage info for the container. 
        self.storage_info = Utils.StorageInfo(input_container_name, output_container_name, full_sas_url_input, full_sas_url_output)        
        print("storage_info", self.storage_info)

        # Upload the asset file that will be rendered and 
        for file in os.listdir("Assets"):        
            if scenefile == file:
                #(blob_client, "fgrp-"+input_container_name, os.getcwd()+"\\Assets\\"+file)
                await loop.run_in_executor(None, functools.partial(Utils.upload_file_to_container, blob_client, "fgrp-"+input_container_name, os.getcwd()+"\\Assets\\"+file))

        

    async def validate(self, batch_service_client):
        loop = asyncio.get_event_loop()
        #self.job_status = await Utils.wait_for_tasks_to_complete(batch_service_client, self.job_id, datetime.timedelta(minutes=30))
        self.job_status = await loop.run_in_executor(None, functools.partial(Utils.wait_for_tasks_to_complete, batch_service_client, self.job_id, datetime.timedelta(minutes=30)))

        # currently broken but this should check to see if job has successfully completed 
        if await self.job_status[0]:  
            self.job_status = await loop.run_in_executor(None, functools.partial(Utils.check_task_output, batch_service_client, self.expected_output))
            print(await self.job_status)

        return self.job_status


    async def delete(self, batch_service_client, blob_client):
        """ 
        Deletes the job, pool and the containers used for the job. If the job fails the output container will not be deleted.
        The non deleted contaienr is used for debugging. 
        """

        #print("self.storage_info: ", self.storage_info)
        #print("Deleting pool: {}. ".format(self.pool_id))
        #batch_service_client.pool.delete(self.pool_id) # need to

        blob_client.delete_container("fgrp-"+self.storage_info.input_container)
        batch_service_client.job.delete(self.job_id)        
        blob_client.delete_container("fgrp-"+self.storage_info.output_container)

        if self.job_status[0]:          
            #print("Deleting job: {}. ".format(self.job_id))
            
            print('Deleting container [{}]...'.format("fgrp-"+self.storage_info.output_container))
            blob_client.delete_container("fgrp-"+self.storage_info.output_container)
        else:
            print("Did not delete the output container")
            print("Job: {}. did not complete successfully, Container {} was not deleted  ".format(self.job_id, "fgrp-"+self.storage_info.output_container))


