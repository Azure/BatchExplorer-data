from xml.etree.ElementTree import Element, SubElement, ElementTree
import logging
import Utils
import time 
import datetime

logger = logging.getLogger('rendering-log')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('template.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(ch)
logger.addHandler(fh)


def info(message):
    logger.info(message)


def error(error_message):
    logger.error(error_message)

def warn(warning_message):
    logger.warn(warning_message)


def account_info(args: object):
    """
    Logs the account info
    :type args: ArgumentParser
    """
    info("-----------------------------------------------------")
    info("------------------Starting runner--------------------")
    info("-----------------------------------------------------")
    info("Batch Account Name: {}".format(args.BatchAccountName))
    info("Batch Account URL: {}".format(args.BatchAccountUrl))
    info("Storage account: {}".format(args.StorageAccountName))
    info("Reading in the list of test in the : {} file".format(args.TestConfig))
    info("-----------------------------------------------------")

def export_result(job_managers, total_time):
    """
    Exports the a file that is that is similar to a pytest export file. This is consumed by
    Azure pipeline to generate a build report.

    :param job_managers: A collection of jobs that were run
    :param timedelta total_time: The duration for all the tasks to complete
      """
    failed_jobs = 0
    info("Exporting test output file")
    root = Element('testsuite')

    for job_item in job_managers:
        child = SubElement(root, "testcase")
        # Add a message to the error
        child.attrib["name"] = str(job_item.raw_job_id)
        if job_item.status.job_state != Utils.JobState.COMPLETE:
            failed_jobs += 1
            sub_child = SubElement(child, "failure")
            sub_child.attrib["message"] = str("Job [{}] failed due the ERROR: [{}]".format(
                job_item.job_id, job_item.status.job_state))

            sub_child.text = str(job_item.status.message)

        # Add the time it took for this test to compete.
        if job_item.duration is not None:
            info("Job {} took {} to complete".format(job_item.job_id, job_item.duration))
            converted_time = time.strptime(str(job_item.duration).split('.')[0],'%H:%M:%S')
            total_seconds = datetime.timedelta(hours=converted_time.tm_hour,minutes=converted_time.tm_min,seconds=converted_time.tm_sec).total_seconds()            
            child.attrib["time"] = str(total_seconds)
        # job did not run, so the test did not run
        else:
            child.attrib["time"] = "0:00:00"

    root.attrib["failures"] = str(failed_jobs)
    root.attrib["tests"] = str(len(job_managers))
    
    root.attrib["time"] = str(total_time.total_seconds())
    tree = ElementTree(root)
    tree.write("Tests/output.xml")


def print_result(job_managers):
    """
    Outputs all the job results into a log file, including their errors and total number of jobs
    that failed and passed
    :param job_managers: The collection of jobs that were run
    :type: list of JobManager
    """
    logger.info("Number of jobs run {}.".format(len(job_managers)))
    failedJobs = 0
    for job_item in job_managers:
        if job_item.status.job_state != Utils.JobState.COMPLETE:
            failedJobs += 1
            logger.info(
                "job {} failed because {} : {}".format(
                    job_item.job_id,
                    job_item.status.job_state,
                    job_item.status.message))

    if failedJobs == 0:
        logger.info("-----------------------------------------")
        logger.info("All jobs ran successfully.")
        logger.info("-----------------------------------------")

    else:
        logger.info("-----------------------------------------")
        logger.info("Number of jobs passed {} out of {}.".format(
        len(job_managers) - failedJobs, len(job_managers)))

