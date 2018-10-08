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

_time = str(datetime.datetime.now().hour) + "-" + str(datetime.datetime.now().minute)

def submit_job(batch_service_client, template, parameters):
    job_json = batch_service_client.job.expand_template(template, parameters)
    print()
    print(job_json)
    job = batch_service_client.job.jobparameter_from_json(job_json)
    batch_service_client.job.add(job)
    

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
    print(parameters_file)
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

class Job(object):
    """docstring for Job"""
    def __init__(self, template_file, pool_template_file, parameters_file, expected_output, storage_info, application_licenses=None):
        super(Job, self).__init__()
        self.job_id = get_job_id(parameters_file)
        self.pool_id = get_pool_id(parameters_file)
        self.template_file = template_file
        self.parameters_file = parameters_file
        self.application_licenses = application_licenses
        self.expected_output = expected_output
        self.pool_template_file = pool_template_file
        self.storage_info = storage_info
    
    def __str__(self):
        return "job_id: {} pool_id:{} ".format(self.job_id, self.pool_id)

    async def Run(self, batch_service_client):
        print('Creating job [{}]...'.format(self.job_id)," job will run on [{}]".format(self.pool_id))
        
        template = ""
        with open(self.template_file) as f: 
            template = json.load(f)
    
        parameters = ""
        with open(self.parameters_file) as f: 
            parameters = json.load(f)
    
        set_parameter_name(parameters, self.job_id)
        set_parameter_storage_info(parameters, self.storage_info)
        
        print(parameters)
        submit_job(batch_service_client, template, parameters)


    def create_pool(self, batch_service_client):
        print('Creating pool [{}]...'.format(self.pool_id))
        template = ""
        with open(self.pool_template_file) as f: 
            template = json.load(f)
            
        if self.application_licenses is not None:
            template["pool"]["applicationLicenses"] = self.application_licenses.split(",")

        all_pools = [p.id for p in batch_service_client.pool.list()]
        
        set_template_name(template, self.pool_id)

        if(self.pool_id not in all_pools):
            pool_json = batch_service_client.pool.expand_template(template)
            pool = batch_service_client.pool.poolparameter_from_json(pool_json)
            batch_service_client.pool.add(pool)
        else:
            print('pool [{}] already exists'.format(self.pool_id))

    def set_rendering_fields(self, pool_template_file, render_version, expected_output, application_licenses=""):
        self.pool_template_file = pool_template_file
        self.render_version = render_version
        self.expected_output = expected_output
        self.applicationLicense = applicationLicense

    async def Validate(self, batch_service_client):
        await Utils.wait_for_tasks_to_complete(batch_service_client, self.job_id, datetime.timedelta(minutes=30))
        return self.job_id, Utils.check_task_output(batch_service_client, self.job_id, self.expected_output)

    async def Delete(self, batch_service_client):
        try:
            print("Deleting job: {}. ".format(self.job_id))
            await batch_service_client.job.delete(self.job_id)
            print("Deleting pool: {}. ".format(self.pool_id))
            await batch_service_client.pool.delete(self.pool_id)
        except batchmodels.batch_error.BatchErrorException as err:
            traceback.print_exc()
            Utils.print_batch_exception(err)
            raise