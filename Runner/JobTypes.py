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

def submit_job(batch_service_client, template, parameters):
    job_json = batch_service_client.job.expand_template(template, parameters)
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

def set_job_template_name(template, job_id):
    try:
        template["parameters"]["jobName"]["defaultValue"] = job_id    
    except KeyError:
        pass
    try:
        template["parameters"]["jobId"]["defaultValue"] = job_id
    except KeyError:
        pass        

def set_job_defaults(template, pool_id, job_id):
    template["parameters"]["poolId"]["defaultValue"] = pool_id
    template["parameters"]["jobName"]["defaultValue"] = job_id
    template["parameters"]["inputData"]["defaultValue"] = "rendering"

def update_template_OutFiles(template_node_outfiles, job_id):
    """
    Adds the prefix ID of the job_id to file group and path. 
    """
    for i in range(0, len(template_node_outfiles)):
        autoStorage = template_node_outfiles[i]["destination"]["autoStorage"]
        autoStorage["fileGroup"] = "rendering-output"
        autoStorage["path"] = autoStorage["path"].replace("[parameters('jobName')]", job_id)  

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
            return True
        else:
            print("job: {} is running".format(job_id))
            await asyncio.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))

def check_task_output(batch_service_client, job_id, expected_output):
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
                return True

    return False, ValueError("cannot find file {} in job {}".format(expected_output, job_id))

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

class Job(object):
    """docstring for Job"""
    pool_id = ""
    def __init__(self, job_id, pool_id, template_file, pool_template_file, parameters_file,  scene_file, isLinux=False):
        super(Job, self).__init__()
        self.job_id = job_id
        self.pool_id = pool_id
        self.template_file = template_file
        self.parameters_file = parameters_file
        self.scene_file = scene_file
        self.isLinux = isLinux
        self.pool_template_file = pool_template_file
    
    def __str__(self):
        return "job_id: {} pool_id:{} scene_file:{} ".format(self.job_id, self.pool_id, self.scene_file)

    async def Run(self, batch_service_client):
        print('Creating job [{}]...'.format(self.job_id)," job will run on [{}]".format(self.pool_id))
        
        template = ""
        with open(self.template_file) as f: 
            template = json.load(f)
    
        parameters = ""
        with open(self.parameters_file) as f: 
            parameters = json.load(f)
    
        set_parameter_name(parameters, self.job_id)
        
        submit_job(batch_service_client, template, parameters)


    def create_pool(self, batch_service_client):
        print('Creating pool [{}]...'.format(self.pool_id))
        template = ""
        with open(self.pool_template_file) as f: 
            template = json.load(f)
    
        set_template_name(template, self.pool_id)
        
        #if self.extra_license is not None:
            #template["pool"]["applicationLicenses"] = self.extra_license

        all_pools = [p.id for p in batch_service_client.pool.list()]

        if(self.pool_id not in all_pools):
            pool_json = batch_service_client.pool.expand_template(template)
            pool = batch_service_client.pool.poolparameter_from_json(pool_json)
            batch_service_client.pool.add(pool)
        else:
            print('pool [{}] already exists'.format(self.pool_id))

    def set_rendering_fields(self, pool_template_file, render_version, expected_output, extra_license=""):
        self.pool_template_file = pool_template_file
        self.render_version = render_version
        self.expected_output = expected_output
        self.extra_license = extra_license

    async def Validate(self, batch_service_client):
        await wait_for_tasks_to_complete(batch_service_client, self.job_id, datetime.timedelta(minutes=30))
        return self.job_id, check_task_output(batch_service_client, self.job_id, self.expected_output)

    async def Delete(self, batchserviceclient):
        try:
            print("Deleting pool: {}. ".format(self.pool_id))
            await batchserviceclient.pool.Delete(self.pool_id)
            print("Deleting job: {}. ".format(self.job_id))
            await batchserviceclient.job.Delete(self.job_id)
        except batchmodels.batch_error.BatchErrorException as err:
            traceback.print_exc()
            print_batch_exception(err)
            raise