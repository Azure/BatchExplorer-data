import os

from azure.batch import BatchServiceClient
from azure.batch import models
from azure.batch.batch_auth import SharedKeyCredentials
from azure.common.credentials import BasicTokenAuthentication, OAuthTokenAuthentication

class Tile:
    def __init__(self, tile, current_x, current_y):
        self.tile = tile
        self.current_x = current_x
        self.current_y = current_y

def main():
    print("------------------------------------")
    print("job manager task reporting for duty")

    # get the environment variables for the job manager task
    x_tiles = int(os.environ["X_TILES"])
    y_tiles = int(os.environ["Y_TILES"])
    frame_start = int(os.environ["FRAME_START"])
    frame_end = int(os.environ["FRAME_END"])
    batch_account_url = os.environ["AZ_BATCH_ACCOUNT_URL"]

    # create Batch client
    # when running inside a task with authentication enabled, this token allows access to the rest of the job
    credentials = OAuthTokenAuthentication(client_id=None, token={"access_token": os.environ["AZ_BATCH_AUTHENTICATION_TOKEN"]})
    batch_client = BatchServiceClient(credentials, base_url=batch_account_url)

    # create the tile collection, can be used for every frame
    tiles = create_tiles(x_tiles, y_tiles)

    # create the task collections for each frame
    current_task_id = 1
    for frame in range(frame_start, frame_end + 1):
        print("generating tasks for frame: {}, with current_task_id: {}".format(frame, current_task_id))
        current_task_id = create_tasks_for_frame(frame, current_task_id, tiles, batch_client)
        print("finished creating tasks for frame: {}, with current_task_id: {}\n".format(frame, current_task_id))


def create_tiles(x_tiles, y_tiles):
    tiles = []
    total = x_tiles * y_tiles
    counter = 0

    print("create_tiles from '{}' -> '{}'".format(0, total))
    for j in range(x_tiles):
        for i in range(y_tiles):
            tiles.append(Tile(counter, i, j))
            counter += 1

    return tiles


def create_tasks_for_frame(frame, current_task_id, tiles, batch_client):
    job_id = os.environ["AZ_BATCH_JOB_ID"]
    depend_start = current_task_id
    tasks = []

    # create a task for every tile
    for tile in tiles:
        print("task: {} - tile: {}, current_x: {}, current_y: {}".format(current_task_id, tile.tile, tile.current_x, tile.current_y))
        tasks.append(create_task(frame, current_task_id, job_id, tile.tile + 1, tile.current_x, tile.current_y))
        current_task_id += 1

    # create merge task to join the tiles back together
    depend_end = current_task_id - 1
    print("merge task: {}, depend_start: {} - depend_end: {}".format(current_task_id, depend_start, depend_end))
    tasks.append(create_merge_task(frame, current_task_id, job_id, depend_start, depend_end))
    current_task_id += 1

    # todo: [tats] - yield return add task collection
    submit_task_collection(batch_client, job_id, tasks, frame)

    return current_task_id
    

def create_task(frame, task_id, job_id, tile_num, current_x, current_y):
    blend_file = os.environ["BLEND_FILE"]
    output_sas = os.environ["OUTPUT_CONTAINER_SAS"]
    optionalParams = os.environ["OPTIONAL_PARAMS"]
    output_format = os.environ["OUTPUT_FORMAT"]

    command_line = "blender -b \"$AZ_BATCH_JOB_PREP_WORKING_DIR/{}\" -P \"$AZ_BATCH_JOB_PREP_WORKING_DIR/python-task-manager.py\" -y -t 0 -F {} -E CYCLES {}".format(blend_file, output_format, optionalParams)    
    return models.TaskAddParameter(
        id=pad_number(task_id),
        display_name="frame: {}, tile: {}".format(frame, tile_num),        
        command_line="/bin/bash -c '{}'".format(command_line),
        environment_settings=[
            models.EnvironmentSetting("X_TILES", os.environ["X_TILES"]),
            models.EnvironmentSetting("Y_TILES", os.environ["Y_TILES"]),
            models.EnvironmentSetting("BLEND_FILE", os.environ["BLEND_FILE"]),
            models.EnvironmentSetting("CURRENT_FRAME", str(frame)),
            models.EnvironmentSetting("CURRENT_TILE", str(tile_num)),
            models.EnvironmentSetting("CURRENT_X", str(current_x)),
            models.EnvironmentSetting("CURRENT_Y", str(current_y))
        ],
        output_files=[
            models.OutputFile(
                file_pattern="../stdout.txt",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/logs/frame-{}/tile-{}.stdout.log".format(job_id, frame, pad_number(tile_num, 3))
                    )
                ),
                upload_options=models.OutputFileUploadOptions(models.OutputFileUploadCondition.task_completion)
            ),
            models.OutputFile(
                file_pattern="../stderr.txt",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/logs/frame-{}/tile-{}.stderr.log".format(job_id, frame, pad_number(tile_num, 3))
                    )
                ),
                upload_options=models.OutputFileUploadOptions(models.OutputFileUploadCondition.task_completion)
            ),
            models.OutputFile(
                file_pattern="../fileuploaderr.txt",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/logs/frame-{}/tile-{}.file_upload_stderr.log".format(job_id, frame, pad_number(tile_num, 3))
                    )
                ),
                upload_options=models.OutputFileUploadOptions(models.OutputFileUploadCondition.task_completion)
            ),
            models.OutputFile(
                file_pattern="../fileuploadout.txt",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/logs/frame-{}/tile-{}.file_upload_stdout.log".format(job_id, frame, pad_number(tile_num, 3))
                    )
                ),
                upload_options=models.OutputFileUploadOptions(models.OutputFileUploadCondition.task_completion)
            ),
            models.OutputFile(
                file_pattern="tile_*",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/outputs/frame-{}".format(job_id, frame)
                    )
                ),
                upload_options=models.OutputFileUploadOptions(models.OutputFileUploadCondition.task_success)
            )   
        ])


def create_merge_task(frame, task_id, job_id, depend_start, depend_end):
    """
    Azure Batch task that executes the ImageMagick `convert` command 
    line to combine all of the output tiles into a single output image.
    This task uses the task dependency model to make sure it 
    doesn't execute before it's dependent tasks have completed. This way 
    we know all of the output image tiles will exist.

    :param frame: Frame number of the scene that this merge task is 
     processing.
    :type id: int
    :param task_id: Identifier of the task.
    :type task_id: str
    :param job_id: Unique identifier of the job. Job identifiers are unique
     within a single Azure Batch account.
    :type job_id: str
    :param depend_start: First task id of the dependency sequence. If each 
     frame is split into 16 tiles, then every 17th task will be a merge task
     and that merge task will be dependent on the preceeding 16 tasks.
     tile tasks 1 - 16, then merge, then tiles 18 - 34, then merge, etc.
    :type depend_start: int
    :param depend_end: Final task id of the dependency sequence. Explanation
     for param `depend_start` applies here as well.
    :type depend_end: int
    """
    x_tiles = int(os.environ["X_TILES"])
    y_tiles = int(os.environ["X_TILES"])
    output_sas = os.environ["OUTPUT_CONTAINER_SAS"]
    working_dir = os.environ["AZ_BATCH_TASK_WORKING_DIR"]
    output_format = os.environ["OUTPUT_FORMAT"]
    print("working_dir: {}".format(working_dir))

    command_line = "convert $AZ_BATCH_TASK_WORKING_DIR/tile_* -flatten frame_{}.{}".format(frame, get_file_extension(output_format))
    print("executing: {}".format(command_line))
    return models.TaskAddParameter(
        id=pad_number(task_id),
        display_name="frame: {} - merge task".format(frame),
        command_line="/bin/bash -c '{}'".format(command_line),
        environment_settings=[
            models.EnvironmentSetting("X_TILES", str(x_tiles)),
            models.EnvironmentSetting("Y_TILES", str(y_tiles))
        ],
        depends_on=models.TaskDependencies(task_id_ranges=[
            models.TaskIdRange(depend_start, depend_end)
        ]),
        resource_files=get_resource_files(x_tiles, y_tiles, frame),
        output_files=[
            models.OutputFile(
                file_pattern="../stdout.txt",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/logs/frame-{}/merge.stdout.log".format(job_id, frame)
                    )
                ),
                upload_options=models.OutputFileUploadOptions(models.OutputFileUploadCondition.task_completion)
            ),
            models.OutputFile(
                file_pattern="../stderr.txt",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/logs/frame-{}/merge.stderr.log".format(job_id, frame)
                    )
                ),
                upload_options=models.OutputFileUploadOptions(models.OutputFileUploadCondition.task_completion)
            ),
            models.OutputFile(
                file_pattern="frame_*",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/outputs/final".format(job_id)
                    )
                ),
                upload_options=models.OutputFileUploadOptions(models.OutputFileUploadCondition.task_success)
            ) 
        ])


def pad_number(number, len=6):
    return str(number).zfill(len)


def get_file_extension(blender_output_format): 
    # just png for now
    return blender_output_format.lower()


def get_resource_files(x_tiles, y_tiles, frame):
    # hard coded to PNG at the moment
    tile_count = x_tiles * y_tiles
    output_format = os.environ["OUTPUT_FORMAT"]
    output_sas = os.environ["OUTPUT_CONTAINER_SAS"]
    job_id = os.environ["AZ_BATCH_JOB_ID"]
    sas_parts = output_sas.split("?")
    files = []

    for num in range(1, tile_count + 1):
        # TODO: Change me to dynamic file extension
        filename = "tile_{}.png".format(str(num).zfill(3))
        path_to_file = "{}/outputs/frame-{}/{}".format(job_id, frame, filename)
        files.append(models.ResourceFile("{}/{}?{}".format(sas_parts[0], path_to_file, sas_parts[1]), filename))

    return files

def submit_task_collection(batch_client, job_id, tasks, frame):
    print("submitting: {} tasks to job: {}, for frame: {}".format(str(len(tasks)), job_id, frame))

    try:
        batch_client.task.add_collection(job_id=job_id, value=tasks)

    except models.BatchErrorException as ex:
        print("got an error adding tasks: {}".format(str(ex)))
        for errorDetail in ex.inner_exception.values:
            print("detail: {}".format(str(errorDetail)))

        raise ex


if __name__ == '__main__':
    main()
