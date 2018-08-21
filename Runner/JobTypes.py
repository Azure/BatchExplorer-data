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

def submit_job(batch_service_client, template):
    """
    Submit job
    """
    job_json = batch_service_client.job.expand_template(template)
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
    def __init__(self, job_id, pool_id, template_file, scene_file, isLinux=False):
        super(Job, self).__init__()
        self.job_id = job_id
        self.pool_id = pool_id
        self.template_file = template_file
        self.scene_file = scene_file
        self.isLinux = isLinux
    
    def __str__(self):
        return "job_id: {} pool_id:{} scene_file:{} ".format(self.job_id, self.pool_id, self.scene_file)

    async def Run(self, batch_service_client):
        print('Creating job [{}]...'.format(self.job_id)," job will run on [{}]".format(self.pool_id))

        with open(self.template_file) as f: 
            template = json.load(f)
        
        template["parameters"]["poolId"]["defaultValue"] = self.pool_id
        template["parameters"]["jobName"]["defaultValue"] = self.job_id
        template["parameters"]["inputData"]["defaultValue"] = "rendering"
        commandLine = template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"]

        if("additionalFlags" in template):
            template["parameters"]["additionalFlags"]["defaultValue"] = "-of png"

        if(self.isLinux):
            newCommandLine = commandLine.replace("[parameters('mayaVersion')]", self.render_version).replace("[parameters('sceneFile')]", self.scene_file)
        else:
            newCommandLine = commandLine.replace("[variables('MayaVersions')[parameters('mayaVersion')].environmentValue]", self.render_version).replace("[parameters('sceneFile')]", self.scene_file)

        template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"] = newCommandLine
            
        update_template_OutFiles(template["job"]["properties"]["taskFactory"]["repeatTask"]["outputFiles"], self.job_id)

        submit_job(batch_service_client, template)

    def create_pool(self, batch_service_client):
        print('Creating pool [{}]...'.format(self.pool_id))
        template = ""
        with open(self.pool_template_file) as f: 
            template = json.load(f)
    
        set_template_name(template, self.pool_id)
        
        if(self.extra_license!=""):
            template["pool"]["applicationLicenses"] = self.extra_license

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


class BlenderJob(Job):
    """docstring for BlenderJob"""
    def __init__(self, job_id, pool_id, template_file, scene_file, isLinux=False):
        Job.__init__(self, job_id, pool_id, template_file, scene_file)
        self.job_id = job_id
        self.pool_id = pool_id
        self.template_file = template_file
        self.scene_file = scene_file
        self.isLinux = isLinux
        
    async def Run(self, batch_service_client):        
        with open(self.template_file) as f: 
            template = json.load(f)
    
        template["parameters"]["poolId"]["defaultValue"] = self.pool_id
        template["parameters"]["jobName"]["defaultValue"] = self.job_id
        template["parameters"]["inputData"]["defaultValue"] = "rendering"
        template["parameters"]["blendFile"]["defaultValue"] = self.scene_file

        if not self.isLinux:
            template["parameters"]["inputDataSas"]["defaultValue"] = "https://mayademoblob.blob.core.windows.net/fgrp-rendering?st=2018-08-13T03%3A37%3A42Z&se=2018-08-20T03%3A52%3A42Z&sp=rl&sv=2018-03-28&sr=c&sig=lpYc5NuYSmJ%2BYGcJyaedSXFe9kZXBuDWMCkAxHnXXBQ%3D"
            
        commandLine = template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"]
        newCommandLine = commandLine.replace("[parameters('jobName')]", self.job_id).replace("[parameters('blendFile')]", self.scene_file)
        template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"] = newCommandLine

        update_template_OutFiles(template["job"]["properties"]["taskFactory"]["repeatTask"]["outputFiles"], self.job_id)
                
        submit_job(batch_service_client, template)        

class VrayJob(Job):
    """docstring for VrayJob"""
    def __init__(self, job_id, pool_id, template_file, scene_file):
        super(VrayJob, self).__init__()
        self.job_id = job_id
        self.pool_id = pool_id
        self.template_file = template_file
        self.scene_file = scene_file
        
    async def Run(self, batch_service_client):
        submit_job(batch_service_client, self.template_file)
            
class Max3ds(Job):
    """docstring for Max3ds"""
    def __init__(self, job_id, pool_id, template_file, max_version, scene_file):
        Job.__init__(self, job_id, pool_id, template_file, scene_file)
        self.job_id = job_id
        self.pool_id = pool_id
        self.template_file = template_file
        self.max_version = max_version

        self.scene_file = scene_file

    def set_rendering_fields(self, pool_template_file, expected_output, renderer, extra_license="", vray_dr_version=""):
        self.pool_template_file = pool_template_file
        self.expected_output = expected_output
        self.renderer = renderer
        self.vray_dr_version = vray_dr_version
        self.extra_license = extra_license

    async def Run(self, batch_service_client):
        print('Creating job [{}]...'.format(self.job_id)," job will run on [{}]".format(self.pool_id))

        with open(self.template_file) as f: 
            template = json.load(f)
        
        print("job",self.renderer)    

        template["parameters"]["poolId"]["defaultValue"] = self.pool_id
        template["parameters"]["jobName"]["defaultValue"] = self.job_id
        template["parameters"]["inputFilegroupSas"]["defaultValue"] = "https://mayademoblob.blob.core.windows.net/fgrp-rendering?st=2018-08-13T03%3A37%3A42Z&se=2018-08-20T03%3A52%3A42Z&sp=rl&sv=2018-03-28&sr=c&sig=lpYc5NuYSmJ%2BYGcJyaedSXFe9kZXBuDWMCkAxHnXXBQ%3D"
        template["parameters"]["sceneFile"]["defaultValue"] = self.scene_file
        template["parameters"]["outputFilegroup"]["defaultValue"] = "rendering-output"
        
        commandLine = ""
        # Use the VRayRT or VRayADV
        if self.vray_dr_version:
            update_template_OutFiles(template["job"]["properties"]["taskFactory"]["tasks"][0]["outputFiles"], self.job_id)
            commandLine = template["job"]["properties"]["taskFactory"]["tasks"][0]["commandLine"]
            newCommandLine = commandLine.replace("[parameters('maxVersion')]", self.max_version).replace("[parameters('renderer')]", self.renderer).replace("[parameters('sceneFile')]", self.scene_file)
            template["job"]["properties"]["taskFactory"]["tasks"][0]["commandLine"] = newCommandLine
            template["job"]["properties"]["properties"] = self.job_id
            template["job"]["properties"]["poolInfo"]["pool_id"] = self.pool_id
            coordinationCommandLine = template["job"]["properties"]["taskFactory"]["tasks"][0]["multiInstanceSettings"]["coordinationCommandLine"]
            newCoordinationCommandLine = coordinationCommandLine.replace("[parameters('vrayRenderer')]", self.vray_dr_version).replace("[parameters('maxVersion')]", self.max_version)
            template["job"]["properties"]["taskFactory"]["tasks"][0]["multiInstanceSettings"]["coordinationCommandLine"] = newCoordinationCommandLine
        
        else: #Use the arnold renderer 
            update_template_OutFiles(template["job"]["properties"]["taskFactory"]["repeatTask"]["outputFiles"], self.job_id)
            commandLine = template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"]
            newCommandLine = commandLine.replace("[parameters('maxVersion')]", self.max_version).replace("[parameters('renderer')]", self.renderer).replace("[parameters('sceneFile')]", self.scene_file)
            template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"] = newCommandLine
        
        submit_job(batch_service_client, template)

class ArnoldJob(Job):
    """used for running V-Ray and arnold stand-alone renderer """
    def __init__(self, job_id, pool_id, template_file, scene_file):
        Job.__init__(self, job_id, pool_id, template_file, scene_file)
        self.job_id = job_id
        self.pool_id = pool_id
        self.template_file = template_file
        self.scene_file = scene_file
        
    async def Run(self, batch_service_client):        
        print('Creating job [{}]...'.format(self.job_id)," job will run on [{}]".format(self.pool_id))

        with open(self.template_file) as f: 
            template = json.load(f)

        template["parameters"]["poolId"]["defaultValue"] = self.pool_id
        template["parameters"]["jobName"]["defaultValue"] = self.job_id
        template["parameters"]["inputData"]["defaultValue"] = "rendering"
        template["parameters"]["sceneFile"]["defaultValue"] = self.scene_file

        commandLine = template["job"]["properties"]["taskFactory"]["tasks"][0]["commandLine"]
        newCommandLine = commandLine.replace("[parameters('sceneFile')]", self.scene_file)
        template["job"]["properties"]["taskFactory"]["tasks"][0]["commandLine"] = newCommandLine

        update_template_OutFiles(template["job"]["properties"]["taskFactory"]["tasks"][0]["outputFiles"], self.job_id)
        submit_job(batch_service_client, template)

class VrayJob(Job):
    """used for running V-Ray and arnold stand-alone renderer """
    def __init__(self, job_id, pool_id, template_file, scene_file):
        Job.__init__(self, job_id, pool_id, template_file, scene_file)
        self.job_id = job_id
        self.pool_id = pool_id
        self.template_file = template_file
        self.scene_file = scene_file
        
    async def Run(self, batch_service_client):        
        print('Creating job [{}]...'.format(self.job_id)," job will run on [{}]".format(self.pool_id))

        with open(self.template_file) as f: 
            template = json.load(f)

        template["parameters"]["poolId"]["defaultValue"] = self.pool_id
        template["parameters"]["jobName"]["defaultValue"] = self.job_id
        template["parameters"]["inputData"]["defaultValue"] = "rendering"
        template["parameters"]["sceneFile"]["defaultValue"] = self.scene_file

        commandLine = template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"]
        newCommandLine = commandLine.replace("[parameters('sceneFile')]", self.scene_file)
        template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"] = newCommandLine

        update_template_OutFiles(template["job"]["properties"]["taskFactory"]["repeatTask"]["outputFiles"], self.job_id)    
        submit_job(batch_service_client, template)

class BlenderTileJob(Job):
    """docstring for VrayJob"""
    def __init__(self, job_id, pool_id, template_file, scene_file):
        Job.__init__(self, job_id, pool_id, template_file, scene_file)
        self.job_id = job_id
        self.pool_id = pool_id
        self.template_file = template_file
        self.scene_file = scene_file
        
    async def Run(self, batch_service_client):
        with open(self.template_file) as f: 
            template = json.load(f)
    
        template["parameters"]["poolId"]["defaultValue"] = self.pool_id
        template["parameters"]["jobName"]["defaultValue"] = self.job_id
        template["parameters"]["inputData"]["defaultValue"] = "rendering"
        template["parameters"]["blendFile"]["defaultValue"] = self.scene_file

#        if not self.isLinux:
        template["parameters"]["inputDataSas"]["defaultValue"] = "https://mayademoblob.blob.core.windows.net/fgrp-rendering?st=2018-08-13T03%3A37%3A42Z&se=2018-08-20T03%3A52%3A42Z&sp=rl&sv=2018-03-28&sr=c&sig=lpYc5NuYSmJ%2BYGcJyaedSXFe9kZXBuDWMCkAxHnXXBQ%3D"
        template["parameters"]["outputSas"]["defaultValue"] = "https://mayademoblob.blob.core.windows.net/fgrp-rendering?st=2018-08-13T03%3A37%3A42Z&se=2018-08-20T03%3A52%3A42Z&sp=rl&sv=2018-03-28&sr=c&sig=lpYc5NuYSmJ%2BYGcJyaedSXFe9kZXBuDWMCkAxHnXXBQ%3D"

        #commandLine = template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"]
        #newCommandLine = commandLine.replace("[parameters('jobName')]", self.job_id).replace("[parameters('blendFile')]", self.scene_file)
        #template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"] = newCommandLine

        update_template_OutFiles(template["job"]["properties"]["jobManagerTask"]["outputFiles"], self.job_id)
                
        submit_job(batch_service_client, template)        
    