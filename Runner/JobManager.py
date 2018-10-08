from azure.storage.blob.models import BlobBlock, ContainerPermissions, ContentSettings
import azure.batch.models as batchmodels

import asyncio
import Utils
import os
import asyncio
import datetime
import functools  
import datetime
import time

async def submit_job(batch_service_client, template, parameters):
    """
    Submits a Job against the batch service.

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param str template: the json desciption of the job
    :param str the parameters of the job
    """
    loop = asyncio.get_event_loop()
    job_json = batch_service_client.job.expand_template(template, parameters)
    jobparameters = batch_service_client.job.jobparameter_from_json(job_json)    
    job = await loop.run_in_executor(None, functools.partial(batch_service_client.job.add, jobparameters))    

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
        self.job_status = Utils.JobStatus(Utils.JobState.NOT_STARTED, "Job hasn't started yet.")

    
    def __str__(self) -> str:
        return "job_id: {} pool_id:{} ".format(self.job_id, self.pool_id)

    async def create_and_submit_Job(self, batch_service_client):
        """
        Creates the Job that will be submitted to the batch service 

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        """
        print('Creating job [{}]...'.format(self.job_id)," job will run on [{}]".format(self.pool_id))
        
        # load the template and parameters file 
        template = Utils.load_file(self.template_file)    
        parameters = Utils.load_file(self.parameters_file)
    
        # overrides some of the parameters needed in the file, container SAS tokens need to be generated for the container
        Utils.set_parameter_name(parameters, self.job_id)
        Utils.set_parameter_storage_info(parameters, self.storage_info)

        # Subsmit the job 
        await submit_job(batch_service_client, template, parameters)


    async def create_pool(self, batch_service_client):
        """
        Creates the Pool that will be submitted to the batch service 

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        """
        # load the template file 
        template = Utils.load_file(self.pool_template_file)    

        # set extra license if needed 
        if self.application_licenses is not None:
            template["pool"]["applicationLicenses"] = self.application_licenses.split(",")

        # Set rendering version 
        Utils.set_image_reference_name(template, "1.2.9", True)

        # List of the pools
        #all_pools = [p.id for p in batch_service_client.pool.list()]
        loop = asyncio.get_event_loop()
        print('Checking to see if pool [{}] exists...'.format(self.pool_id))

        all_pools = [p.id for p in await loop.run_in_executor(None, functools.partial(batch_service_client.pool.list))]
        
        Utils.set_template_name(template, self.pool_id)
        if(self.pool_id not in all_pools):
            pool_json = batch_service_client.pool.expand_template(template)
            pool = batch_service_client.pool.poolparameter_from_json(pool_json)
            print('Creating pool [{}]...'.format(self.pool_id))
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

        full_sas_url_input = 'https://{}.blob.core.windows.net/{}?{}'.format(blob_client.account_name, "fgrp-"+input_container_name, Utils.get_container_sas_token(blob_client, "fgrp-"+input_container_name, ContainerPermissions.READ + ContainerPermissions.LIST))
        full_sas_url_output = 'https://{}.blob.core.windows.net/{}?{}'.format(blob_client.account_name, "fgrp-"+output_container_name, Utils.get_container_sas_token(blob_client, "fgrp-"+output_container_name, ContainerPermissions.READ + ContainerPermissions.LIST + ContainerPermissions.WRITE))
        
        # Set the storage info for the container. 
        self.storage_info = Utils.StorageInfo(input_container_name, output_container_name, full_sas_url_input, full_sas_url_output)        

        # Upload the asset file that will be rendered and 
        for file in os.listdir("Assets"):        
            if scenefile == file:
                #(blob_client, "fgrp-"+input_container_name, os.getcwd()+"\\Assets\\"+file)
                await loop.run_in_executor(None, functools.partial(Utils.upload_file_to_container, blob_client, "fgrp-"+input_container_name, os.getcwd()+"\\Assets\\"+file))


    async def check_expected_output(self, batch_service_client):
        """
        Checks to see if the the job expected output is correct

        """
        loop = asyncio.get_event_loop()

        if self.job_status.job_state == Utils.JobState.COMPLETE:
            self.job_status = await loop.run_in_executor(None, Utils.check_task_output, batch_service_client, self.job_id, self.expected_output)  

    def check_pool_state(self, batch_service_client):
        pool = batch_service_client.pool.get(self.pool_id)
        #print("pool", pool)
        
        while pool.allocation_state.value == "resizing":
            time.sleep(5)            
            self.check_pool_state(batch_service_client)

        # check if pool 
        if pool.allocation_state.value == "steady":
            if pool.resize_errors != None: 
                self.job_status = Utils.JobStatus(Utils.JobState.FAILED, "Job failed to start since the pool [{}] failed to allocate any TVMs due to error [Code: {}, message {}].".format(self.pool_id, pool.resize_errors[0].code, pool.resize_errors[0].message))
                print("POOL FAILED TO ALLOCATE")
                return False
                    
            if pool.current_dedicated_nodes == pool.target_dedicated_nodes:
                return True

    async def wait_for_tasks_to_complete(self, batch_service_client, timeout):
        """
        Wait for tasks to complete, and set the job status. 

        """
        loop = asyncio.get_event_loop()
        # Wait for all the tasks to complete
         
        if await loop.run_in_executor(None, self.check_pool_state, batch_service_client):
            self.job_status = await loop.run_in_executor(None, Utils.wait_for_tasks_to_complete, batch_service_client, self.job_id, datetime.timedelta(minutes=timeout))

            await self.check_expected_output(batch_service_client)

    async def delete(self, batch_service_client, blob_client):
        """ 
        Deletes the job, pool and the containers used for the job. If the job fails the output container will not be deleted.
        The non deleted contaienr is used for debugging. 
        """
        loop = asyncio.get_event_loop()
        # delete the pool 
        # print("Deleting pool: {}. ".format(self.pool_id))        
        # batch_service_client.pool.delete(self.pool_id) # need to

        # delete the job 
        print("Deleting job: {}. ".format(self.job_id))
        #await loop.run_in_executor(None, functools.partial(batch_service_client.job.delete, self.job_id))    


        print('Deleting container [{}]...'.format("fgrp-"+self.storage_info.input_container))
        blob_client.delete_container("fgrp-"+self.storage_info.input_container)
        #await loop.run_in_executor(None, blob_client.delete_container, "fgrp-"+self.storage_info.input_container)
        
        #blob_client.delete_container("fgrp-"+self.storage_info.output_container)
        
        if self.job_status.job_state == "COMPLETE":          
            print('Deleting container [{}]...'.format("fgrp-"+self.storage_info.output_container))
            await loop.run_in_executor(None, blob_client.delete_container, "fgrp-"+self.storage_info.output_container)
        else:
            print("Did not delete the output container")
            print("Job: {}. did not complete successfully, Container {} was not deleted  ".format(self.job_id, "fgrp-"+self.storage_info.output_container))