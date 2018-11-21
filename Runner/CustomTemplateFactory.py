# -*- coding: utf-8 -*-
import json
"""
This module responsibility is for reading and setting elements of the json templates
"""


def set_template_pool_id(template, pool_id):
    """
    Finds the correct poolName or poolId and sets the value based on the pool_id

    :param template: The json file that needs to be changed
    :param str pool_id: The value that needs to be set 
    """

    # Since these are nested we have to do some deeper digging. 
    if template.get("parameters").get("poolName") != None:
        template["parameters"]["poolName"]["defaultValue"] = pool_id
    
    elif template.get("parameters").get("poolId") != None: 
        if template.get("parameters").get("poolId").get('defaultValue') != None:
            template["parameters"]["poolId"]["defaultValue"] = pool_id
        
        elif template.get("parameters").get("poolId").get('value') != None:
            template["parameters"]["poolId"]["value"] = pool_id    


def set_parameter_name(template, job_id):
    """
    Finds the correct jobName or jobId and sets the value based on the job_id

    :param template: The json file that needs to be changed
    :param job_id: The value that needs to be set
    """
    if 'jobName' in template:
        template["jobName"]["value"] = job_id

    elif 'jobId' in template:
        template["jobId"]["value"] = job_id


def set_parameter_storage_info(template, storage_info):
    """
    Finds the correct input data or inputFilegroup  and sets the value based on the storage_info

    :param template: The json file that needs to be changed
    :param storage_info: 'Utils.StorageInfo'
    """

    # 'fgrp-' needs to be removed.
    if 'inputData' in template:
        template["inputData"]["value"] = storage_info.input_container.replace("fgrp-", "")
    elif 'inputFilegroup' in template:
        template["inputFilegroup"]["value"] = storage_info.input_container.replace("fgrp-", "")

    # Set file group SAS input
    if 'inputFilegroupSas' in template:
        template["inputFilegroupSas"]["value"] = storage_info.input_container_SAS
    elif 'inputDataSas' in template:
        template["inputDataSas"]["value"] = storage_info.input_container_SAS

    # Set output filegroup
    if 'outputFilegroup' in template:
        template["outputFilegroup"]["value"] = storage_info.output_container.replace("fgrp-", "")
    elif 'outputs' in template:
        template["outputs"]["value"] = storage_info.output_container.replace("fgrp-", "")

    if 'outputSas' in template:
        template["outputSas"]["value"] = storage_info.output_container_SAS
        

def set_image_reference_properties(template, image_ref):
    """
    Sets what rendering image the tests are going to run on. 

    :param template: The json file that needs to be changed
    :param image_ref: 'Utils.ImageReference'
    """
    if 'version' in template:
        template["version"] = image_ref.version

    if 'offer' in template:
        template["offer"] = image_ref.offer


def set_image_reference(template, image_ref):
    """
    Sets what rendering image the tests are going to run on. 

    :param template: The json file that needs to be changed
    :param image_reference: 'Utils.ImageReference'
    """
    template_image_reference = template["variables"]["osType"]["imageReference"]

    # If the image is not a rendering image then no action needs to happen on
    # the pool template
    if template_image_reference["publisher"] != "batch":
        return

    # If template is windows version
    if "windows" in template_image_reference["offer"]:
        for i in range(0, len(image_ref)):
            if image_ref[i].osType == "windows":
                set_image_reference_properties(template_image_reference, image_ref[i])

    # if the template is centos
    if "centos" in template_image_reference["offer"]:
        for i in range(0, len(image_ref)):
            if image_ref[i].osType == "liunx":
                set_image_reference_properties(template_image_reference, image_ref[i])


def get_job_id(parameters_file: str) -> str:
    """
    Gets the job id from the parameters json file. 

    :param parameters_file: The parameters json file we want to load. 
    :rtype: str
    :return: The job id that is in the parameters
    """
    job_id = ""
    if parameters_file == None:
        return "empty-job"

    with open(parameters_file) as f:
        parameters = json.load(f)
        if 'jobName' in parameters:
            job_id = parameters["jobName"]["value"]
        elif 'jobId' in parameters:
            job_id = parameters["jobId"]["value"]

    return job_id


def get_pool_id(parameters_file: str) -> str:
    """
    Gets the pool id from the parameters json file. 

    :param parameters_file: The parameters json file we want to load. 
    :rtype: str
    :return: The pool id that is in the parameters
    """
    if parameters_file == None:
        return "empty-pool"
    pool_id = ""

    with open(parameters_file) as f:
        parameters = json.load(f)
        if 'poolName' in parameters:
            pool_id = parameters["poolName"]["value"]
        elif 'poolId' in parameters:
            pool_id = parameters["poolId"]["value"]

    return pool_id


def get_scene_file(parameters_file: str) -> str:
    """
    Gets the scene file from the parameters json file. 

    :param parameters_file: The parameters json file we want to load. 
    :rtype: str
    :return: The scene file that is in the parameters
    """
    with open(parameters_file) as f:
        parameters = json.load(f)
        if 'sceneFile' in parameters:
            scene_file = parameters["sceneFile"]["value"]

        elif 'blendFile' in parameters:
            scene_file = parameters["blendFile"]["value"]
 
    return scene_file


def load_file(template_file: str) -> str:
    """
    load the a file 

    :param template_file: The template file. 
    :rtype: str
    :return: loads the json from a file into memory 
    """
    with open(template_file) as f:
        template = json.load(f)

    return template
