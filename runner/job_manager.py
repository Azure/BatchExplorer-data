from azure.storage.blob.models import ContainerPermissions
from pathlib import Path
import azext.batch as batch
import azure.storage.blob as azureblob
import azure.batch.models as batchmodels
import traceback
import utils
import os
import datetime
import time
import custom_template_factory as ctm
import logger

"""
This module is responsible for creating, submitting and monitoring the pools and jobs

"""

_time = str(datetime.datetime.now().day) + "-" + \
        str(datetime.datetime.now().hour) + "-" + \
        str(datetime.datetime.now().minute)





class JobManager(object):

    def __init__(self, template_file: str, pool_template_file: str,
                 parameters_file: str, expected_output: str, application_licenses: str = None):
        super(JobManager, self).__init__()
        self.raw_job_id = ctm.get_job_id(parameters_file)  # The attribute 'raw_job_id' of type 'str'
        self.job_id = _time + "-" + self.raw_job_id  # The attribute 'job_id' of type 'str'
        self.pool_id = _time + "-" + ctm.get_pool_id(parameters_file)  # The attribute 'pool_id' of type 'str'
        self.template_file = template_file  # The attribute 'template_file' of type 'str'
        self.parameters_file = parameters_file  # The attribute 'parameters_file' of type 'str '
        self.application_licenses = application_licenses  # The attribute 'application_licenses' of type 'str'
        self.expected_output = expected_output  # The attribute 'expected_output' of type 'str'
        self.pool_template_file = pool_template_file  # The attribute 'pool_template_file' of type 'str'
        self.storage_info = None  # The attribute 'storage_info' of type 'utils.StorageInfo'
        self.status = utils.JobStatus(
            utils.JobState.NOT_STARTED,
            "Job hasn't started yet.")  # The attribute 'status' of type 'utils.JobState'
        self.duration = None  # The attribute 'duration' of type 'int'

    def __str__(self) -> str:
        return "job_id: [{}] pool_id: [{}] ".format(self.job_id, self.pool_id)
    
    def submit_job(self, batch_service_client: batch.BatchExtensionsClient, template: str, parameters: str):
        """
        Submits a Job against the batch service.

        :param batch_service_client: The batch client to use.
        :type batch_service_client: `azure.batch.BatchExtensionsClient`
        :param template: The json description of the job
        :type template: str
        :param parameters: The job parameters of the job
        :type parameters: str
        """
        try:
            job_json = batch_service_client.job.expand_template(
                template, parameters)
            job_parameters = batch_service_client.job.jobparameter_from_json(
                job_json)
            batch_service_client.job.add(job_parameters)
        except batchmodels.batch_error.BatchErrorException as err:
            logger.error(
                "Failed to submit job\n{}\n with params\n{}".format(
                    template, parameters))
            traceback.print_exc()
            utils.print_batch_exception("Error: {}".format(err))
            raise
        except batch.errors.MissingParameterValue as mpv:
            logger.error("Job {}, failed to submit, because of the error: {}".format(self.raw_job_id, mpv))
            raise
        except:
            logger.error("Job {}, failed to submit, because of the error: {}".format(self.raw_job_id, sys.exc_info()[0]))
            raise

    def create_and_submit_job(self, batch_client: batch.BatchExtensionsClient):
        """
        Creates the Job that will be submitted to the batch service

        :param batch_client: The batch client to use.
        :type batch_client: `azure.batch.BatchExtensionsClient`
        """
        logger.info('Creating Job [{}]... job will run on [{}]'.format(self.job_id, self.pool_id))

        # load the template and parameters file
        template = ctm.load_file(self.template_file)
        parameters = ctm.load_file(self.parameters_file)

        # overrides some of the parameters needed in the file, container SAS
        # tokens need to be generated for the container
        ctm.set_parameter_name(parameters, self.job_id)
        ctm.set_parameter_storage_info(parameters, self.storage_info)
        ctm.set_template_pool_id(parameters, self.pool_id)

        # Submits the job
        self.submit_job(batch_client, template, parameters)

    def submit_pool(self, batch_service_client: batch.BatchExtensionsClient, template: str):
        """
        Submits a batch pool based on the template 

        :param batch_service_client: The batch client used for making batch operations
        :type batch_service_client: `azure.batch.BatchExtensionsClient`
        :param template: The in memory version of the template used to create a the job.
        :type template: str
        """
        parameters = ctm.load_file(self.parameters_file)
        pool_json = batch_service_client.pool.expand_template(template, parameters)
        ctm.set_template_pool_id(template, self.pool_id)
        pool = batch_service_client.pool.poolparameter_from_json(pool_json)
        logger.info('Creating pool [{}]...'.format(self.pool_id))
        try:
            batch_service_client.pool.add(pool)
        except batchmodels.batch_error.BatchErrorException as err:
            if utils.expected_exception(
                    err, "The specified pool already exists"):
                logger.warning(
                    "Pool [{}] is already being created.".format(
                        self.pool_id))
            else:
                logger.info("Create pool error: {}".format(err))
                traceback.print_exc()
                utils.print_batch_exception(err)

    def create_pool(self, batch_service_client: batch.BatchExtensionsClient,
                    image_references: 'List[utils.ImageReference]', VM_image_URL=None, VM_OS_type=None):
        """
        Creates the Pool that will be submitted to the batch service.

        :type batch_service_client: `azure.batch.BatchExtensionsClient`
        :param batch_service_client: The batch client used for making batch operations
        :type image_references: List['utils.ImageReference`]
        :param image_references: A list of image references that job can run
        :type VM_image_URL: str
        :param VM_image_URL: The resource link to an image inside your image repo. If this is resource link is provided
        the VMs will use the custom image you provided.
        :type VM_OS_type: str
        :param VM_OS_type: The custom image operating system type, this can be windows or centos. This is needed if you
        want to use a custom image.
        """

        # load the template file
        template = ctm.load_file(self.pool_template_file)

        # set extra license if needed
        if self.application_licenses is not None:
            template["pool"]["applicationLicenses"] = self.application_licenses.split(",")

        # Set rendering version
        ctm.set_image_reference(template, image_references)
        ctm.set_template_pool_id(template, self.pool_id)
        if VM_image_URL is not None and VM_OS_type is not None:
            ctm.set_custom_image(template, VM_image_URL, VM_OS_type)

        all_pools = [p.id for p in batch_service_client.pool.list()]

        if self.pool_id not in all_pools:
            self.submit_pool(batch_service_client, template)
        else:
            logger.info('pool [{}] already exists'.format(self.pool_id))

    def upload_assets(self, blob_client: azureblob.BlockBlobService):
        """
        Uploads a the file specified in the json parameters file into a storage container that will 
        delete it's self after 7 days 

        :param blob_client: A blob service client used for making blob operations.
        :type blob_client: `azure.storage.blob.BlockBlobService`
        """
        input_container_name = "fgrp-" + self.job_id
        output_container_name = "fgrp-" + self.job_id + '-output'

        # Create input container
        blob_client.create_container(input_container_name, fail_on_exist=False)
        logger.info('creating a storage container: {}'.format(input_container_name))

        # Create output container
        blob_client.create_container(output_container_name, fail_on_exist=False)
        logger.info('creating a storage container: {}'.format(output_container_name))

        full_sas_url_input = 'https://{}.blob.core.windows.net/{}?{}'.format(
            blob_client.account_name,
            input_container_name,
            utils.get_container_sas_token(
                blob_client,
                input_container_name,
                ContainerPermissions.READ +
                ContainerPermissions.LIST))
        full_sas_url_output = 'https://{}.blob.core.windows.net/{}?{}'.format(
            blob_client.account_name,
            output_container_name,
            utils.get_container_sas_token(
                blob_client,
                output_container_name,
                ContainerPermissions.READ +
                ContainerPermissions.LIST +
                ContainerPermissions.WRITE))

        # Set the storage info for the container.
        self.storage_info = utils.StorageInfo(
            input_container_name,
            output_container_name,
            full_sas_url_input,
            full_sas_url_output)

        # Upload the asset file that will be rendered and
        scenefile = ctm.get_scene_file(self.parameters_file)
        for file in os.listdir("Assets"):
            if scenefile == file:
                file_path = Path("Assets/" + file)
                utils.upload_file_to_container(blob_client, input_container_name, file_path)

    def check_expected_output(self, batch_service_client: batch.BatchExtensionsClient):
        """
        Checks to see if the the job expected output is correct

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchExtensionsClient`
        """
        if self.status.job_state == utils.JobState.COMPLETE:
            self.status = utils.check_task_output(batch_service_client, self.job_id, self.expected_output)

    def check_for_pool_resize_error(self, pool: str) -> bool:
        """
        Checks to see if there is any pool resize errors. Returns true if there is one, false if there isn't

        :param pool: The
        :type pool: The pool we want to inspect for any timeout errors
        :return: True if there is a resize error
        :rtype: bool 
        """
        if pool.allocation_state.value == "steady" and pool.resize_errors is not None:
            self.status = utils.JobStatus(utils.JobState.POOL_FAILED,
                                          "Job failed to start since the pool [{}] failed to allocate any TVMs due to "
                                          "error [Code: {}, message {}]. "
                                          .format(self.pool_id, pool.resize_errors[0].code,
                                                  pool.resize_errors[0].message))
            logger.error("POOL {} FAILED TO ALLOCATE".format(self.pool_id))
            return True
        return False

    def check_time_has_expired(self, timeout: datetime.timedelta) -> bool:
        """
        Checks to see if the current time is less than the timeout and returns True if timeout hasn't been reached
        :param timeout: The duration we wait for task the complete.
        :type timeout: datetime.timedelta
        :return: Returns a True if the timeout hasn't been reached.
        :rtype: bool.
        """
        timeout_expiration = datetime.datetime.now() + timeout
        return datetime.datetime.now() <= timeout_expiration

    def wait_for_steady_tvm(self, batch_service_client: batch.BatchExtensionsClient,
                            timeout: datetime.timedelta) -> bool:
        """
        This method will wait until the pool has TVM available to run the job. 

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchExtensionsClient`
        :param timedelta timeout: The duration we wait for task complete.
        :return: Returns true when their is a valid TVM in an idle state
        :rtype bool
        """
        pool = batch_service_client.pool.get(self.pool_id)

        # Wait for pool to come up 
        while pool.allocation_state.value == "resizing" and self.check_time_has_expired(timeout):
            time.sleep(10)
            pool = batch_service_client.pool.get(self.pool_id)

        # Check if pool allocated with a resize errors. 
        if self.check_for_pool_resize_error(pool):
            return False

        # Wait for TVMs to become available 
        # Need to cast to a list here since compute_node.list returns an object that contains a list 
        nodes = list(batch_service_client.compute_node.list(self.pool_id))

        logger.info("Waiting for a TVM to allocate in pool: [{}]".format(self.pool_id))
        while (any([n for n in nodes if n.state != batchmodels.ComputeNodeState.idle])) and self.check_time_has_expired(
                timeout):
            time.sleep(10)
            nodes = list(batch_service_client.compute_node.list(self.pool_id))

        if any([n for n in nodes if n.state == batchmodels.ComputeNodeState.idle]):
            logger.info("Job [{}] is starting to run on a TVM".format(self.job_id))
            return True
        else:
            self.job_status = utils.JobStatus(utils.JobState.POOL_FAILED,
                                              "Failed to start the pool [{}] before [{}], you may want to increase your timeout].".format(
                                                  self.pool_id, timeout))
            logger.error("POOL [{}] FAILED TO ALLOCATE IN TIME".format(self.pool_id))
            return False

    def wait_for_job_results(self, batch_service_client: batch.BatchExtensionsClient, timeout: int):
        """
        Wait for tasks to complete, and set the job status.

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchExtensionsClient`
        :param timeout: The duration we wait for task complete.
        :type timeout: int
        """
        # Starts the timer
        self.duration = time.time()

        # Wait for all the tasks to complete
        if self.wait_for_steady_tvm(batch_service_client, datetime.timedelta(minutes=timeout)):
            # How long it takes for the pool to start up
            pool_time = time.time() - self.duration

            job_time = time.time()
            self.status = utils.wait_for_tasks_to_complete(batch_service_client, self.job_id,
                                                           datetime.timedelta(minutes=timeout))
            # How long the Job runs for
            job_time = time.time() - job_time

            # How long it took for both the pool and job time to start.
            self.duration = (datetime.timedelta(seconds=(pool_time + job_time)))
            self.check_expected_output(batch_service_client)

    def retry(self, batch_service_client: batch.BatchExtensionsClient, blob_client: azureblob.BlockBlobService,
              timeout: int):
        """
        Retries a job if it failed due to a NOT_COMPLETE or UNEXPECTED_OUTPUT. If the pool fails we don't retry the job

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchExtensionsClient`
        :param blob_client: A blob service client.
        :type blob_client: `azure.storage.blob.BlockBlobService`
        :param timeout: How long we should wait for the job to complete
        :type timeout: int
        """
        if self.status.job_state == utils.JobState.NOT_COMPLETE or self.status.job_state == utils.JobState.UNEXPECTED_OUTPUT:
            # Deletes the resources needed for the old job.
            self.delete_resources(batch_service_client, blob_client, True)
            logger.warning(
                "Job [{}] did not complete in time so it will be recreated with the '-retry' postfix ".format(
                    self.job_id))
            # Set a new job id
            self.job_id = self.job_id + "-retry"
            self.upload_assets(blob_client)
            self.create_and_submit_job(batch_service_client)
            self.wait_for_job_results(batch_service_client, timeout)

    def delete_pool(self, batch_service_client: batch.BatchExtensionsClient):
        """
        Deletes the pool the if the pool, if the pool has already been deleted or marked for deletion it
        should ignore the batch exception that is thrown. These errors come up due to multiple jobs using the same pool
        and when a the job cleans up after it's self it will call delete on the same pool since they are a shared resource.

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchExtensionsClient`
        """
        logger.info("Deleting pool: {}.".format(self.pool_id))
        try:
            batch_service_client.pool.delete(self.pool_id)
        except batchmodels.batch_error.BatchErrorException as batch_exception:
            if utils.expected_exception(
                    batch_exception, "The specified pool has been marked for deletion"):
                logger.warning(
                    "The specified pool [{}] has been marked for deletion.".format(
                        self.pool_id))
            elif utils.expected_exception(batch_exception, "The specified pool does not exist"):
                logger.warning(
                    "The specified pool [{}] has been deleted.".format(
                        self.pool_id))
            else:
                traceback.print_exc()
                utils.print_batch_exception(batch_exception)

    def delete_resources(self, batch_service_client: batch.BatchExtensionsClient,
                         blob_client: azureblob.BlockBlobService, force_delete: bool = None):
        """
        Deletes the job, pool and the containers used for the job. If the job fails the output container will not be deleted.
        The non deleted container is used for debugging.

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchExtensionsClient`
        :param blob_client: A blob service client.
        :type blob_client: `azure.storage.blob.BlockBlobService`
        :param force_delete: Forces the deletion of all the containers related this job.
        :type force_delete: bool
        """
        # delete the job
        try:
            batch_service_client.job.delete(self.job_id)
        except batchmodels.batch_error.BatchErrorException as batch_exception:
            if utils.expected_exception(
                    batch_exception, "The specified job does not exist"):
                logger.error(
                    "The specified Job [{}] was not created.".format(
                        self.job_id))
            else:
                traceback.print_exc()
                utils.print_batch_exception(batch_exception)

        if self.status.job_state in {
            utils.JobState.COMPLETE, utils.JobState.POOL_FAILED, utils.JobState.NOT_STARTED} or force_delete:
            logger.info('Deleting container [{}]...'.format(
                self.storage_info.input_container))
            blob_client.delete_container(self.storage_info.input_container)

            logger.info('Deleting container [{}]...'.format(
                self.storage_info.output_container))
            blob_client.delete_container(self.storage_info.output_container)
        else:
            logger.info("Did not delete the output container")
            logger.info(
                "Job: {}. did not complete successfully, Container {} was not deleted.".format(
                    self.job_id, self.storage_info.output_container))
