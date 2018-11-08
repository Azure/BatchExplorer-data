
---------------------------------
---------------------------------
------- Batch Labs Runner -------
---------------------------------
---------------------------------

This python script is used for testing the Batch Templates inside the BatchExplorer-data\ncj folder. It does not test BatchExplorer only the Azure Batch CLI and its extension client. Please set the following environmental variables before running the job script. 

PS_BATCH_ACCOUNT_NAME
PS_BATCH_ACCOUNT_KEY
PS_BATCH_ACCOUNT_URL
PS_BATCH_ACCOUNT_SUB
PS_STORAGE_ACCOUNT_NAME
PS_STORAGE_ACCOUNT_KEY
PS_SERVICE_PRINCIPAL_CREDENTIALS_CLIENT_ID
PS_SERVICE_PRINCIPAL_CREDENTIALS_SECRET
PS_SERVICE_PRINCIPAL_CREDENTIALS_TENANT
PS_SERVICE_PRINCIPAL_CREDENTIALS_RESOUCE

To run the python script you first need to install it's dependencies by using the following command

> npm install 
> pip3 install -r python/requirements.txt # or pip if on windows or only have python 3.6 installed

After installing the dependencies, you can just run the program through the command line and specify a manifest file to run like so

> python .\Runner.py "Tests/TestConfiguration.json"

The manifest file contains 5 properties you need to set and 1 optional.  

{
  "tests": [
    {
        "name": "name of the test, this is only the display name of the job”
        "template": "../ncj/maya/render-default-windows/job.template.json", # A link to the template you want to run
        "poolTemplate": "../ncj/maya/render-default-windows/pool.template.json", A link to the pool template you want the job to run on 
        "parameters": "Tests/maya/render-default-windows/job.parameters2017.json", # A job parameters file that all the parameters that need to be set on the Job
        "expectedOutput": "maya.exr.0001" What the expected output is meant to be in the task, this is used for validation
		"applicationLicense": "if any addadtional licenses need to be set"
    }
}

IMPORTANT 
If you want to use a new rendering image you need to update the json in the TestConfiguration.json file. 
