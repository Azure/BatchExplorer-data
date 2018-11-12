# -*- coding: utf-8 -*-
import json
"""
This module responsiblilty is for reading and setting elements of the json templates
"""


def set_template_name(template, pool_id):
    try:
        template["parameters"]["poolName"]["defaultValue"] = pool_id
    except KeyError:
        pass
    try:
        template["parameters"]["poolId"]["defaultValue"] = pool_id
    except KeyError:
        pass
    try:
        template["parameters"]["poolId"]["value"] = pool_id
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
    # Set input filegroup
    """
    'fgrp-' needs to be removed.
    """
    try:
        template["inputData"]["value"] = storage_info.input_container.replace("fgrp-", "")
    except KeyError:
        pass
    try:
        template["inputFilegroup"]["value"] = storage_info.input_container.replace("fgrp-", "")
    except KeyError:
        pass

    # Set file group SAS input
    try:
        template["inputFilegroupSas"]["value"] = storage_info.input_container_SAS
    except KeyError:
        pass
    try:
        template["inputDataSas"]["value"] = storage_info.input_container_SAS
    except KeyError:
        pass

    # Set output filegroup
    try:
        template["outputFilegroup"]["value"] = storage_info.output_container.replace("fgrp-", "")
    except KeyError:
        pass
    try:
        template["outputs"]["value"] = storage_info.output_container.replace("fgrp-", "")
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


def set_image_reference_properties(template, image_reference):
    try:
        template["version"] = image_reference.version
    except KeyError:
        pass

    try:
        template["offer"] = image_reference.offer
    except KeyError:
        pass


def set_image_reference(template, image_ref):
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
    job_id = ""

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
    return job_id


def get_pool_id(parameters_file: str) -> str:
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


def get_scene_file(parameters_file: str) -> str:
    with open(parameters_file) as f:
        parameters = json.load(f)
    try:
        sceneFile = parameters["sceneFile"]["value"]
    except KeyError:
        pass
    try:
        sceneFile = parameters["blendFile"]["value"]
    except KeyError:
        pass

    return sceneFile


def load_file(template_file: str) -> str:
    template = ""
    with open(template_file) as f:
        template = json.load(f)

    return template
