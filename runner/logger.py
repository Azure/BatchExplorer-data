from xml.etree.ElementTree import Element, SubElement, ElementTree
import logging
import utils
import time
import datetime

logger = logging.getLogger('rendering-log')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
file_handler = logging.FileHandler('template.log')
file_handler.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(file_handler)


def info(message: str):
    """
    Log 'msg % args' with severity 'INFO' to the logger file

    :param message: The info message that will be added to the logger file
    :type message: str
    """
    logger.info(message)


def error(error_message: str):
    """
    Log 'msg % args' with severity 'ERROR' to the logger file

    :param error_message: The info message that will be added to the logger file
    :type error_message: str
    """
    logger.error(error_message)


def warning(warning_message: str):
    """
    Log 'msg % args' with severity 'ERROR' to the logger file

    :param warning_message: The info message that will be added to the logger file
    :type warning_message: str
    """
    logger.warning(warning_message)


def account_info(args: object):
    """
    Logs the account info

    :param args: A few of the arguments set on the command line
    :type args: ArgumentParser
    """
    info("Batch Account Name: {}".format(args.BatchAccountName))
    info("Batch Account URL: {}".format(args.BatchAccountUrl))
    info("Storage account: {}".format(args.StorageAccountName))
    info("Reading in the list of test in the : {} file".format(args.TestConfig))


def export_result(job_managers: 'list[job_manager.JobManager]', total_time: int):
    """
    Exports the a file that is that is similar to a pytest export file. This is consumed by
    Azure pipeline to generate a build report.

    :param job_managers: A collection of jobs that were run
    :type job_managers: List[job_managers.JobManager]
    :param total_time: The duration for all the tasks to complete
    :type total_time: int
      """
    failed_jobs = 0  # type: int
    info("Exporting test output file")
    root = Element('testsuite')

    for job_item in job_managers:
        child = SubElement(root, "testcase")
        # Add a message to the error
        child.attrib["name"] = str(job_item.raw_job_id)
        if job_item.status.job_state != utils.JobState.COMPLETE:
            failed_jobs += 1
            sub_child = SubElement(child, "failure")
            sub_child.attrib["message"] = str("Job [{}] failed due the ERROR: [{}]".format(
                job_item.job_id, job_item.status.job_state))

            sub_child.text = str(job_item.status.message)

        # Add the time it took for this test to compete.
        if job_item.duration is not None:
            info("Job {} took {} to complete".format(job_item.job_id, job_item.duration))
            # If the job failed we set the duration to 0
            job_duration = "0:00:00"
            try:
                converted_time = time.strptime(str(job_item.duration).split('.')[0], '%H:%M:%S')
                total_seconds = datetime.timedelta(hours=converted_time.tm_hour, minutes=converted_time.tm_min,
                                               seconds=converted_time.tm_sec).total_seconds()
            except ValueError as e:
                child.attrib["time"] = job_duration
                    
            child.attrib["time"] = str(total_seconds)
        # job did not run, so the test did not run
        else:
            child.attrib["time"] = "0:00:00"

    root.attrib["failures"] = str(failed_jobs)
    root.attrib["tests"] = str(len(job_managers))

    root.attrib["time"] = str(total_time.total_seconds())
    tree = ElementTree(root)
    tree.write("Tests/output.xml")


def print_result(job_managers: 'list[job_manager.JobManager]'):
    """
    Outputs all the results of the jobs into a log file, including their errors and the total number of jobs
    that failed and passed

    :param job_managers: The collection of jobs that were run
    :type job_managers: List[job_managers.JobManager]
    """
    info("Number of jobs run {}.".format(len(job_managers)))
    failed_jobs = 0  # type: int
    for job_item in job_managers:
        if job_item.status.job_state != utils.JobState.COMPLETE:
            failed_jobs += 1
            warning(
                "job {} failed because {} : {}".format(
                    job_item.job_id,
                    job_item.status.job_state,
                    job_item.status.message))

    if failed_jobs == 0:
        info("All jobs ran successfully.")

    else:
        info("Number of jobs passed {} out of {}.".format(
            len(job_managers) - failed_jobs, len(job_managers)))
