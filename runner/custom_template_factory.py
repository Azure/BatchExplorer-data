# -*- coding: utf-8 -*-
import json

"""
This module responsibility is for reading and setting elements of the json templates in memory.
"""


def set_template_pool_id(in_memory_json_object: str, pool_id: str):
    """
    Finds the poolName or poolId inside the in_memory_json_object and sets the value based on the pool_id

    :param in_memory_json_object: The json object that needs to be updated with a new pool id 
    :type in_memory_json_object: str
    :param pool_id: The value that needs to be set
    :type pool_id: str
    """

    # Since these are nested we have to do some deeper digging. 
    if in_memory_json_object.get("parameters") is None:
        if in_memory_json_object.get("poolId"):
            in_memory_json_object["poolId"]["value"] = pool_id

        elif in_memory_json_object.get("poolName"):
            in_memory_json_object["poolName"]["value"] = pool_id

    elif in_memory_json_object.get("parameters"):
        if in_memory_json_object.get("parameters").get("poolName"):
            in_memory_json_object["parameters"]["poolName"]["defaultValue"] = pool_id

        elif in_memory_json_object.get("parameters").get("poolId"):
            if in_memory_json_object.get("parameters").get("poolId").get('defaultValue'):
                in_memory_json_object["parameters"]["poolId"]["defaultValue"] = pool_id

            elif in_memory_json_object.get("parameters").get("poolId").get('value'):
                in_memory_json_object["parameters"]["poolId"]["value"] = pool_id

    if in_memory_json_object.get("pool") is not None: 
        if in_memory_json_object.get("id") is None:
            in_memory_json_object["pool"]["id"] = pool_id


def set_custom_image(in_memory_json_object: str, VM_image_URL: str, VM_image_type: str):
    """
    Sets what the custom image the tests are going to run on. 

    :param in_memory_json_object: The json object that needs to be updated with a version and offer type for the images 
    :type in_memory_json_object: str
    :param VM_image_URL: The resource link to an image inside your image repo.
    :type VM_image_URL: 'str'
    :param VM_image_type: The custom image operating system type.
    :type VM_image_URL: 'str'
    """

    if in_memory_json_object.get("variables").get("osType").get("imageReference") is not None:
        del in_memory_json_object["variables"]["osType"]["imageReference"]["publisher"]
        del in_memory_json_object["variables"]["osType"]["imageReference"]["offer"]
        del in_memory_json_object["variables"]["osType"]["imageReference"]["sku"]
        del in_memory_json_object["variables"]["osType"]["imageReference"]["version"]
        in_memory_json_object["variables"]["osType"]["imageReference"]["virtualMachineImageId"] = VM_image_URL

    # TODO For handling windows and centos the nodeAgentSKUId needs to be changed in the json send object
    # The centos image hasn't been tested so I haven't written logic for handling the VM_image_type


def set_parameter_name(in_memory_json_object: str, job_id: str):
    """
    Finds the jobName or jobId inside the in_memory_json_object and sets the value based on the job manager job_id

    :param in_memory_json_object: The json object that needs to be updated with a new job id or name 
    :type in_memory_json_object :str
    :param job_id: The value that needs to be set
    :type job_id: str
    """
    if in_memory_json_object.get("jobName") is not None:
        in_memory_json_object["jobName"]["value"] = job_id

    elif in_memory_json_object.get("jobId") is not None:
        in_memory_json_object["jobId"]["value"] = job_id


def set_parameter_storage_info(in_memory_json_object: str, storage_info: str):
    """
    Finds the input data or inputFilegroup inside the in_memory_json_object and sets the value based on the storage_info

    :param in_memory_json_object: The json object that needs to be updated with a new storage location 
    :type in_memory_json_object: str
    :param storage_info: A storage object that links to input and output containers that the job needs to run
    :type storage_info: 'utils.StorageInfo'
    """

    # 'fgrp-' needs to be removed.
    if in_memory_json_object.get("inputData") is not None:
        in_memory_json_object["inputData"]["value"] = storage_info.input_container.replace("fgrp-", "")
    elif in_memory_json_object.get("inputFilegroup") is not None:
        in_memory_json_object["inputFilegroup"]["value"] = storage_info.input_container.replace("fgrp-", "")

    # Set file group SAS input
    if in_memory_json_object.get("inputFilegroupSas") is not None:
        in_memory_json_object["inputFilegroupSas"]["value"] = storage_info.input_container_SAS
    elif in_memory_json_object.get("inputDataSas") is not None:
        in_memory_json_object["inputDataSas"]["value"] = storage_info.input_container_SAS

    # Set output file group
    if in_memory_json_object.get("outputFilegroup") is not None:
        in_memory_json_object["outputFilegroup"]["value"] = storage_info.output_container.replace("fgrp-", "")
    elif in_memory_json_object.get("outputs") is not None:
        in_memory_json_object["outputs"]["value"] = storage_info.output_container.replace("fgrp-", "")

    if in_memory_json_object.get("outputSas") is not None:
        in_memory_json_object["outputSas"]["value"] = storage_info.output_container_SAS


def set_image_reference_properties(in_memory_json_object: str, image_ref: 'utils.ImageReference'):
    """
    Sets what rendering image the tests are going to run on. 

    :param in_memory_json_object: The json object that needs to be updated with a version and offer type for the images 
    :type in_memory_json_object: str
    :param image_ref: The new image reference used for creating a pool
    :type image_ref: 'utils.ImageReference'
    """
    if 'version' in in_memory_json_object:
        in_memory_json_object["version"] = image_ref.version

    if 'offer' in in_memory_json_object:
        in_memory_json_object["offer"] = image_ref.offer


def set_image_reference(in_memory_json_object: str, image_ref: 'List[utils.ImageReference]'):
    """
    Sets what rendering image the test is going to run on.

    :param in_memory_json_object: The json object that needs to be updated with a new image reference 
    :param image_ref: A list of image references that the test can run on.
    :type image_ref: List[utils.ImageReference]
    """
    image_reference = in_memory_json_object["variables"]["osType"]["imageReference"]

    # If the image is not a rendering image then no action needs to happen on
    # the pool json_object
    if image_reference.get("publisher") != "batch":        
        return

    # If json_object is windows version
    if "windows" in image_reference["offer"]:
        for i in range(0, len(image_ref)):
            if image_ref[i].osType == "windows":
                set_image_reference_properties(image_reference, image_ref[i])

    # if the json_object is Centos
    if "centos" in image_reference["offer"]:
        for i in range(0, len(image_ref)):
            if image_ref[i].osType == "liunx":
                set_image_reference_properties(image_reference, image_ref[i])


def get_job_id(parameters_file: str) -> str:
    """
    Gets the job id from the parameters json file. 

    :param parameters_file: The parameters json file we want to load.
    :type parameters_file: str
    :return: The job id that is in the parameters
    :rtype: str
    """
    job_id = ""
    if parameters_file is None:
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
    :type parameters_file: str
    :return: The pool id that is in the parameters file.
    :rtype: str
    """
    if parameters_file is None:
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
    Gets the scene file from the parameters file.

    :param parameters_file: The parameters json file we want to load.
    :type parameters_file: str
    :return: The name of the scene file that is in the parameters file
    :rtype: str
    """
    with open(parameters_file) as f:
        parameters = json.load(f)
        if 'sceneFile' in parameters:
            scene_file = parameters["sceneFile"]["value"]

        elif 'blendFile' in parameters:
            scene_file = parameters["blendFile"]["value"]

    return scene_file


def load_file(template_file_location: str) -> str:
    """
    loads the file and returns the loaded file in memory

    :param template_file_location: The template file.
    :type template_file_location: str
    :rtype: str
    :return: loads the json from a file into memory 
    """
    with open(template_file_location) as f:
        template = json.load(f)

    return template
