import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels
import azext.batch as batch 
import json

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

class Job(object):
    """docstring for Job"""
    pool_id = ""
    def __init__(self, job_id, pool_id, template_file, scene_file):
        super(Job, self).__init__()
        self.job_id = job_id
        self.pool_id = pool_id
        self.template_file = template_file
        self.scene_file = scene_file
    
    def __str__(self):
        return "job_id: {} pool_id:{} scene_file:{} ".format(self.job_id, self.pool_id, self.scene_file)

    def Run(self, batch_service_client, render_version, isLinux=False):
        print('Creating job [{}]...'.format(self.job_id)," job will run on [{}]".format(self.pool_id))

        #job = batch.models.JobAddParameter(self.job_id, batch.models.PoolInformation(pool_id=pool_id))

        with open(self.template_file) as f: 
            template = json.load(f)
        
        template["parameters"]["poolId"]["defaultValue"] = self.pool_id
        template["parameters"]["jobName"]["defaultValue"] = self.job_id
        template["parameters"]["inputData"]["defaultValue"] = "rendering"
        commandLine = template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"]

        if("additionalFlags" in template):
            template["parameters"]["additionalFlags"]["defaultValue"] = "-of png"

        if(isLinux):
            newCommandLine = commandLine.replace("[parameters('mayaVersion')]", render_version).replace("[parameters('sceneFile')]", self.scene_file)
        else:
            newCommandLine = commandLine.replace("[variables('MayaVersions')[parameters('mayaVersion')].environmentValue]", render_version).replace("[parameters('sceneFile')]", self.scene_file)

        template["job"]["properties"]["taskFactory"]["repeatTask"]["commandLine"] = newCommandLine
            
        update_template_OutFiles(template["job"]["properties"]["taskFactory"]["repeatTask"]["outputFiles"], self.job_id)
        print(template)

        submit_job(batch_service_client, self.template_file)

    def create_pool(self, batch_service_client, pool_template_file, extra_license=""):
        print('Creating pool [{}]...'.format(self.pool_id))
        template = ""
        with open(pool_template_file) as f: 
            template = json.load(f)
    
        set_template_name(template, self.pool_id)
        
        if(extra_license!=""):
            template["pool"]["applicationLicenses"] = extra_license

        all_pools = [p.id for p in batch_service_client.pool.list()]

        if(self.pool_id not in all_pools):
            pool_json = batch_service_client.pool.expand_template(template)
            pool = batch_service_client.pool.poolparameter_from_json(pool_json)
            batch_service_client.pool.add(pool)
        else:
            print('pool [{}] already exists'.format(self.pool_id))
        
        

class BlenderJob(Job):
    """docstring for BlenderJob"""
    def __init__(self, job_id, pool_id, template_file, scene_file):
        super(BlenderJob, self).__init__()
        self.job_id = job_id
        self.pool_id = pool_id
        self.template_file = template_file
        self.scene_file = scene_file
        
    def Run(batch_service_client):
        submit_job(batch_service_client, self.template_file)
        

class VrayJob(Job):
    """docstring for VrayJob"""
    def __init__(self, job_id, pool_id, template_file, scene_file):
        super(VrayJob, self).__init__()
        self.job_id = job_id
        self.pool_id = pool_id
        self.template_file = template_file
        self.scene_file = scene_file
        
    def Run(batch_service_client):
        submit_job(batch_service_client, self.template_file)
        


