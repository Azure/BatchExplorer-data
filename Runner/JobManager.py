from azure.storage.blob.models import BlobBlock, ContainerPermissions, ContentSettings
from pathlib import Path
import azure.batch.models as batchmodels
import traceback
import asyncio
import Utils
import os
import datetime
import functools
import datetime
import time
import CustomTemplateFactory as ctm
"""
This module is responsible for creating, submitting and monitoring the pools and jobs

"""


_time = str(datetime.datetime.now().day) + "-" + \
    str(datetime.datetime.now().hour) + "-" + \
    str(datetime.datetime.now().minute)


async def submit_job(batch_service_client, template, parameters):
    """
    Submits a Job against the batch service.

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param str template: the json desciption of the job
    :param str the parameters of the job
    """
    try:
        loop = asyncio.get_event_loop()
        job_json = batch_service_client.job.expand_template(
            template, parameters)
        jobparameters = batch_service_client.job.jobparameter_from_json(
            job_json)
        job = await loop.run_in_executor(None, functools.partial(batch_service_client.job.add, jobparameters))
    except batchmodels.batch_error.BatchErrorException as err:
        print(
            "Failed to submit job\n{}\n with params\n{}".format(
                template, parameters))
        traceback.print_exc()
        Utils.print_batch_exception(err)
        raise


class JobManager(object):

    def __init__(self, template_file, pool_template_file,
                 parameters_file, expected_output, application_licenses=None):
        super(JobManager, self).__init__()
        self.raw_job_id = ctm.get_job_id(parameters_file)
        self.job_id = _time + "-" + self.raw_job_id
        self.pool_id = ctm.get_pool_id(parameters_file)
        self.template_file = template_file
        self.parameters_file = parameters_file
        self.application_licenses = application_licenses
        self.expected_output = expected_output
        self.pool_template_file = pool_template_file
        self.storage_info = None
        self.job_status = Utils.JobStatus(
            Utils.JobState.NOT_STARTED,
            "Job hasn't started yet.")
        self.duration = None

    def __str__(self) -> str:
        return "job_id: [{}] pool_id: [{}] ".format(self.job_id, self.pool_id)

    async def create_and_submit_Job(self, batch_service_client):
        """
        Creates the Job that will be submitted to the batch service

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        """
        print(
            'Creating Job [{}]...'.format(
                self.job_id), " job will run on [{}]".format(
                self.pool_id))

        # load the template and parameters file
        template = ctm.load_file(self.template_file)
        parameters = ctm.load_file(self.parameters_file)

        # overrides some of the parameters needed in the file, container SAS
        # tokens need to be generated for the container
        ctm.set_parameter_name(parameters, self.job_id)
        ctm.set_parameter_storage_info(parameters, self.storage_info)

        # Submits the job
        await submit_job(batch_service_client, template, parameters)

    async def create_pool(self, batch_service_client, image_references):
        """
        Creates the Pool that will be submitted to the batch service

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        """
        # load the template file
        template = ctm.load_file(self.pool_template_file)

        # set extra license if needed
        if self.application_licenses is not None:
            template["pool"]["applicationLicenses"] = self.application_licenses.split(
                ",")

        # Set rendering version
        ctm.set_image_reference(template, image_references)

        loop = asyncio.get_event_loop()
        all_pools = [p.id for p in await loop.run_in_executor(None, functools.partial(batch_service_client.pool.list))]

        ctm.set_template_name(template, self.pool_id)
        if(self.pool_id not in all_pools):
            pool_json = batch_service_client.pool.expand_template(template)
            pool = batch_service_client.pool.poolparameter_from_json(pool_json)
            print('Creating pool [{}]...'.format(self.pool_id))
            try:
                await loop.run_in_executor(None, functools.partial(batch_service_client.pool.add, pool))
            except batchmodels.batch_error.BatchErrorException as err:
                if Utils.expected_exception(
                        err, "The specified pool already exists"):
                    print(
                        "Pool [{}] is already being created.".format(
                            self.pool_id))
                else:
                    print("Create pool error: ", err)
                    traceback.print_exc()
                    Utils.print_batch_exception(err)
        else:
            print('pool [{}] already exists'.format(self.pool_id))

    async def upload_assets(self, blob_client):
        loop = asyncio.get_event_loop()
        input_container_name = "fgrp-" + self.job_id
        output_container_name = "fgrp-" + self.job_id + '-output'

        # Create input container
        await loop.run_in_executor(None, functools.partial(blob_client.create_container, input_container_name, fail_on_exist=False))

        # Create output container
        await loop.run_in_executor(None, functools.partial(blob_client.create_container, output_container_name, fail_on_exist=False))

        full_sas_url_input = 'https://{}.blob.core.windows.net/{}?{}'.format(
            blob_client.account_name,
            input_container_name,
            Utils.get_container_sas_token(
                blob_client,
                input_container_name,
                ContainerPermissions.READ +
                ContainerPermissions.LIST))
        full_sas_url_output = 'https://{}.blob.core.windows.net/{}?{}'.format(
            blob_client.account_name,
            output_container_name,
            Utils.get_container_sas_token(
                blob_client,
                output_container_name,
                ContainerPermissions.READ +
                ContainerPermissions.LIST +
                ContainerPermissions.WRITE))

        # Set the storage info for the container.
        self.storage_info = Utils.StorageInfo(
            input_container_name,
            output_container_name,
            full_sas_url_input,
            full_sas_url_output)

        # Upload the asset file that will be rendered and
        scenefile = ctm.get_scene_file(self.parameters_file)
        for file in os.listdir("Assets"):
            if scenefile == file:
                filePath = Path("Assets/" + file)
                await loop.run_in_executor(None, functools.partial(Utils.upload_file_to_container, blob_client, input_container_name, filePath))

    async def check_expected_output(self, batch_service_client):
        """
        Checks to see if the the job expected output is correct

        """
        loop = asyncio.get_event_loop()

        if self.job_status.job_state == Utils.JobState.COMPLETE:
            self.job_status = await loop.run_in_executor(None, Utils.check_task_output, batch_service_client, self.job_id, self.expected_output)

    def check_pool_resize_error(self, pool):
        if pool.allocation_state.value == "steady" and pool.resize_errors is not None:
            self.job_status = Utils.JobStatus(Utils.JobState.POOL_FAILED,
                                              "Job failed to start since the pool [{}] failed to allocate any TVMs due to error [Code: {}, message {}]."
                                              .format(self.pool_id, pool.resize_errors[0].code, pool.resize_errors[0].message))
            print("POOL {} FAILED TO ALLOCATE".format(self.pool_id))
            return True
        return False

    def check_timeout(self, timeout):
        timeout_expiration = datetime.datetime.now() + timeout
        return datetime.datetime.now() <= timeout_expiration

    def check_pool_state(self, batch_service_client, timeout):
        pool = batch_service_client.pool.get(self.pool_id)

        # wait for pool to come up
        while pool.allocation_state.value == "resizing" and self.check_timeout(
                timeout):
            time.sleep(10)
            pool = batch_service_client.pool.get(self.pool_id)

        # Check if pool allocated with a resize errors.
        if self.check_pool_resize_error(pool):
            return False

        # Wait for TVMs to become available
        nodes = list(batch_service_client.compute_node.list(self.pool_id))

        print(
            "Waiting for a TVM to allocate in pool: [{}]".format(
                self.pool_id))
        while (any([n for n in nodes if n.state != batchmodels.ComputeNodeState.idle])
               ) and self.check_timeout(timeout):
            time.sleep(10)
            nodes = list(batch_service_client.compute_node.list(self.pool_id))

        if any([n for n in nodes if n.state ==
                batchmodels.ComputeNodeState.idle]):
            print("Job [{}] is starting to run on a TVM".format(self.job_id))
            return True
        else:
            self.job_status = Utils.JobStatus(Utils.JobState.POOL_FAILED,
                                              "Failed to start the pool [{}] before [{}], you may want to increase your timeout].".format(self.pool_id, timeout))
            print("POOL [{}] FAILED TO ALLOCATE IN TIME".format(self.pool_id))
            return False

    async def wait_for_tasks_to_complete(self, batch_service_client, timeout):
        """
        Wait for tasks to complete, and set the job status.

        """
        loop = asyncio.get_event_loop()
        # start the timer
        self.duration = time.time()

        # Wait for all the tasks to complete
        if await loop.run_in_executor(None, self.check_pool_state, batch_service_client, datetime.timedelta(minutes=timeout)):

            # How long it takes for the pool to start up
            poolTime = time.time() - self.duration

            jobTime = time.time()
            self.job_status = await loop.run_in_executor(None, Utils.wait_for_tasks_to_complete, batch_service_client, self.job_id, datetime.timedelta(minutes=timeout))
            # How long the Job runs for
            jobTime = time.time() - jobTime

            # How long it took for both the pool and job time to start.
            self.duration = (datetime.timedelta(seconds=(poolTime + jobTime)))
            await self.check_expected_output(batch_service_client)

    async def retry(self, batch_service_client, blob_client, timeout):
        """
        Retries a job if it failed due to a NOT_COMPLETE or UNEXPECTED_OUTPUT. If the pool fails we don't retry the job
        """
        loop = asyncio.get_event_loop()
        if self.job_status.job_state == Utils.JobState.NOT_COMPLETE or self.job_status.job_state == Utils.JobState.UNEXPECTED_OUTPUT:
            # Deletes the resources needed for the old job.
            await self.delete_resouces(batch_service_client, blob_client, True)
            print(
                "Job [{}] did not complete in time so it will be recreated with the '-retry' postfix ".format(self.job_id))
            # Set a new job id
            self.job_id = self.job_id + "-retry"
            await self.upload_assets(blob_client)
            await self.create_and_submit_Job(batch_service_client)
            await self.wait_for_tasks_to_complete(batch_service_client, timeout)

    async def delete_pool(self, batch_service_client):
        """
        Deletes the pool the if the pool, if the pool has already been deleted or marked for deletion it
        should ignore the batch exception that is thrown. These errors come up due to multiple jobs using the same pool
        and when a the job cleans up after it's self it will call delete on the same pool since they are a shared resource.
        """
        print("Deleting pool: {}.".format(self.pool_id))
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, functools.partial(batch_service_client.pool.delete, self.pool_id))
        except batchmodels.batch_error.BatchErrorException as batch_exception:
            if Utils.expected_exception(
                    batch_exception, "The specified pool has been marked for deletion"):
                print(
                    "The specified pool [{}] has been marked for deletion.".format(
                        self.pool_id))
            elif Utils.expected_exception(batch_exception, "The specified pool does not exist"):
                print(
                    "The specified pool [{}] has been deleted.".format(
                        self.pool_id))
            else:
                print
                traceback.print_exc()
                Utils.print_batch_exception(batch_exception)

    async def delete_resouces(self, batch_service_client, blob_client, force_delete=False):
        """
        Deletes the job, pool and the containers used for the job. If the job fails the output container will not be deleted.
        The non deleted container is used for debugging.
        """
        loop = asyncio.get_event_loop()
        # delete the job
        try:
            await loop.run_in_executor(None, functools.partial(batch_service_client.job.delete, self.job_id))
        except batchmodels.batch_error.BatchErrorException as batch_exception:
            if Utils.expected_exception(
                    batch_exception, "The specified job does not exist"):
                print(
                    "The specified Job [{}] was not created.".format(
                        self.job_id))
            else:
                print
                traceback.print_exc()
                Utils.print_batch_exception(batch_exception)

        if self.job_status.job_state in {
                Utils.JobState.COMPLETE, Utils.JobState.POOL_FAILED, Utils.JobState.NOT_STARTED} or force_delete:
            print('Deleting container [{}]...'.format(
                self.storage_info.input_container))
            blob_client.delete_container(self.storage_info.input_container)

            print('Deleting container [{}]...'.format(
                self.storage_info.output_container))
            await loop.run_in_executor(None, blob_client.delete_container, self.storage_info.output_container)
        else:
            print("Did not delete the output container")
            print(
                "Job: {}. did not complete successfully, Container {} was not deleted.".format(
                    self.job_id, self.storage_info.output_container))
