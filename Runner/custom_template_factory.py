# -*- coding: utf-8 -*-
import json

"""
This module responsibility is for reading and setting elements of the json templates in memory.
"""


def set_template_pool_id(json_object: str, pool_id: str):
    """
    Finds the poolName or poolId inside the json_object and sets the value based on the pool_id

    :param json_object: The json object that needs to be updated with a new pool id 
    :type json_object: str
    :param str pool_id: The value that needs to be set
    :type pool_id: str
    """

    # Since these are nested we have to do some deeper digging. 
    if json_object.get("parameters").get("poolName") is not None:
        json_object["parameters"]["poolName"]["defaultValue"] = pool_id

    elif json_object.get("parameters").get("poolId") is not None:
        if json_object.get("parameters").get("poolId").get('defaultValue') is not None:
            json_object["parameters"]["poolId"]["defaultValue"] = pool_id

        elif json_object.get("parameters").get("poolId").get('value') is not None:
            json_object["parameters"]["poolId"]["value"] = pool_id


def set_parameter_name(json_object: str, job_id: str):
    """
    Finds the jobName or jobId inside the json_object and sets the value based on the job manager job_id

    :param json_object: The json object that needs to be updated with a new job id or name 
    :type json_object :str
    :param job_id: The value that needs to be set
    :type job_id: str
    """
    if json_object.get("jobName") is not None:
        json_object["jobName"]["value"] = job_id

    elif json_object.get("jobId") is not None:
        json_object["jobId"]["value"] = job_id


def set_parameter_storage_info(json_object: str, storage_info: str):
    """
    Finds the input data or inputFilegroup inside the json_object and sets the value based on the storage_info

    :param json_object: The json object that needs to be updated with a new storage location 
    :type json_object: str
    :param storage_info: A storage object that links to input and output containers that the job needs to run
    :type storage_info: Utils.StorageInfo
    """

    # 'fgrp-' needs to be removed.
    if json_object.get("inputData") is not None:
        json_object["inputData"]["value"] = storage_info.input_container.replace("fgrp-", "")
    elif json_object.get("inputFilegroup") is not None:
        json_object["inputFilegroup"]["value"] = storage_info.input_container.replace("fgrp-", "")

    # Set file group SAS input
    if json_object.get("inputFilegroupSas") is not None:
        json_object["inputFilegroupSas"]["value"] = storage_info.input_container_SAS
    elif json_object.get("inputDataSas") is not None:
        json_object["inputDataSas"]["value"] = storage_info.input_container_SAS

    # Set output filegroup
    if json_object.get("outputFilegroup") is not None:
        json_object["outputFilegroup"]["value"] = storage_info.output_container.replace("fgrp-", "")
    elif json_object.get("outputs") is not None:
        json_object["outputs"]["value"] = storage_info.output_container.replace("fgrp-", "")

    if json_object.get("outputSas") is not None:
        json_object["outputSas"]["value"] = storage_info.output_container_SAS


def set_image_reference_properties(json_object: str, image_ref: 'List[util.ImageReference]'):
    """
    Sets what rendering image the tests are going to run on. 

    :param json_object: The json object that needs to be updated with a version and offer type for the images 
    :type json_object: str
    :param image_ref: The new image reference used for creating a pool
    :type image_ref: 'Utils.ImageReference'
    """
    if 'version' in json_object:
        json_object["version"] = image_ref.version

    if 'offer' in json_object:
        json_object["offer"] = image_ref.offer


def set_image_reference(json_object: str, image_ref: 'List[util.ImageReference]'):
    """
    Sets what rendering image the test is going to run on.

    :param json_object: The json object that needs to be updated with a new image reference 
    :param image_ref: A list of image references that the test can run on.
    :type  image_ref: List[Utils.ImageReference]
    """
    json_object_image_reference = json_object["variables"]["osType"]["imageReference"]

    # If the image is not a rendering image then no action needs to happen on
    # the pool json_object
    if json_object_image_reference["publisher"] is not "batch":
        return

    # If json_object is windows version
    if "windows" in json_object_image_reference["offer"]:
        for i in range(0, len(image_ref)):
            if image_ref[i].osType == "windows":
                set_image_reference_properties(json_object_image_reference, image_ref[i])

    # if the json_object is centos
    if "centos" in json_object_image_reference["offer"]:
        for i in range(0, len(image_ref)):
            if image_ref[i].osType == "liunx":
                set_image_reference_properties(json_object_image_reference, image_ref[i])


def get_job_id(parameters_file: str) -> str:
    """
    Gets the job id from the parameters json file. 

    :param parameters_file: The parameters json file we want to load.
    :type parameters_file: str
    :rtype: str
    :return: The job id that is in the parameters
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
    :rtype: str
    :return: The pool id that is in the parameters
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
