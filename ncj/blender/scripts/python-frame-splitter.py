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

PAD_LEN_ID = 6
PAD_LEN_FRAME = 4
PAD_LEN_TILE = 3

def main():
    print("-----------------------------------")
    print("job manager task reporting for duty")
    print("-----------------------------------")

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
    """
    Generate the list of tiles based on the number of X and Y tiles 
    that have been defined.

    :param x_tiles: Number of tiles on the X axis.
    :type x_tiles: int
    :param y_tiles: Number of tiles on the Y axis.
    :type y_tiles: int
    """
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
    """
    Given the collection of tiles and the frame number, create a Azure Batch
    task for each tile and a merge task to stitch them back together again. 
    Also takes the current task id so we can keep track of how many tasks we 
    have created.

    :param frame: Current frame number to render.
    :type frame: int
    :param current_task_id: Current task id. For every tile task and merge task
     created, this counter gets incremented.
    :type current_task_id: int
    :param tiles: Collection of tiles that we generated earlier.
    :type tiles: list[~Tile]
    :param batch_client: Client for issuing REST requests to the Azure Batch
     service.
    :type batch_client: ~BatchServiceClient
    """
    job_id = os.environ["AZ_BATCH_JOB_ID"]
    depend_start = current_task_id
    tasks = []

    # create a task for every tile
    for tile in tiles:
        tasks.append(create_task(frame, current_task_id, job_id, tile.tile + 1, tile.current_x, tile.current_y))
        print("task: {} - tile: {}, current_x: {}, current_y: {}".format(current_task_id, tile.tile, tile.current_x, tile.current_y))
        current_task_id += 1

    # create merge task to join the tiles back together
    depend_end = current_task_id - 1
    print("merge task: {}, depend_start: {} - depend_end: {}".format(current_task_id, depend_start, depend_end))
    tasks.append(create_merge_task(frame, current_task_id, job_id, depend_start, depend_end))
    current_task_id += 1

    # TODO: [tats] - yield return add task collection
    submit_task_collection(batch_client, job_id, tasks, frame)

    return current_task_id
    

def create_task(frame, task_id, job_id, tile_num, current_x, current_y):
    """
    Azure Batch task that renders the given tile. Run Blender from the command 
    line and pass in the job manager script and the blend file. 

    :param frame: Frame number of the scene that this merge task is 
     processing.
    :type frame: int
    :param task_id: Identifier of the task.
    :type task_id: str
    :param job_id: Unique identifier of the job. Job identifiers are unique
     within a single Azure Batch account.
    :type job_id: str
    :param tile_num: Number of the current tile.
    :type tile_num: int
    :param current_x: X value of the current tile, used to generate the render
     border.
    :type current_x: int
    :param current_y: Y value of the current tile, used to generate the render
     border.
    :type current_y: int
    """
    blend_file = os.environ["BLEND_FILE"]
    output_sas = os.environ["OUTPUT_CONTAINER_SAS"]
    optionalParams = os.environ["OPTIONAL_PARAMS"]
    command_line = blender_command(blend_file, optionalParams)

    # only print this once
    if task_id == 1:
        print("tile task command line: {}".format(command_line))

    return models.TaskAddParameter(
        id=pad_number(task_id, PAD_LEN_ID),
        display_name="frame: {}, tile: {}".format(frame, tile_num),
        command_line=os_specific_command_line(command_line),
        constraints=models.TaskConstraints(max_task_retry_count = 2),
        environment_settings=[
            models.EnvironmentSetting("X_TILES", os.environ["X_TILES"]),
            models.EnvironmentSetting("Y_TILES", os.environ["Y_TILES"]),
            models.EnvironmentSetting("CROP_TO_BORDER", os.environ["CROP_TO_BORDER"]),
            models.EnvironmentSetting("OUTPUT_FORMAT", os.environ["OUTPUT_FORMAT"]),
            models.EnvironmentSetting("BLEND_FILE", os.environ["BLEND_FILE"]),
            models.EnvironmentSetting("CURRENT_FRAME", str(frame)),
            models.EnvironmentSetting("CURRENT_TILE", str(tile_num)),
            models.EnvironmentSetting("CURRENT_X", str(current_x)),
            models.EnvironmentSetting("CURRENT_Y", str(current_y))
        ],
        resource_files=[
            models.ResourceFile(
                "https://raw.githubusercontent.com/Azure/BatchExplorer-data/master/ncj/blender/scripts/python-task-manager.py",
                "scripts/python-task-manager.py"
            )
        ],
        output_files=[
            models.OutputFile(
                file_pattern="../stdout.txt",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/logs/frame-{}/tile-{}.stdout.log".format(job_id, pad_number(frame, PAD_LEN_FRAME), pad_number(tile_num, PAD_LEN_TILE))
                    )
                ),
                upload_options=models.OutputFileUploadOptions(models.OutputFileUploadCondition.task_completion)
            ),
            models.OutputFile(
                file_pattern="../stderr.txt",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/logs/frame-{}/tile-{}.stderr.log".format(job_id, pad_number(frame, PAD_LEN_FRAME), pad_number(tile_num, PAD_LEN_TILE))
                    )
                ),
                upload_options=models.OutputFileUploadOptions(models.OutputFileUploadCondition.task_completion)
            ),
            models.OutputFile(
                file_pattern="../fileuploaderr.txt",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/logs/frame-{}/tile-{}.file_upload_stderr.log".format(job_id, pad_number(frame, PAD_LEN_FRAME), pad_number(tile_num, PAD_LEN_TILE))
                    )
                ),
                upload_options=models.OutputFileUploadOptions(models.OutputFileUploadCondition.task_completion)
            ),
            models.OutputFile(
                file_pattern="../fileuploadout.txt",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/logs/frame-{}/tile-{}.file_upload_stdout.log".format(job_id, pad_number(frame, PAD_LEN_FRAME), pad_number(tile_num, PAD_LEN_TILE))
                    )
                ),
                upload_options=models.OutputFileUploadOptions(models.OutputFileUploadCondition.task_completion)
            ),
            models.OutputFile(
                file_pattern="tile_*",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/outputs/frame-{}".format(job_id, pad_number(frame, PAD_LEN_FRAME))
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
    :type frame: int
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

    # crop to border means we need to use montage to tile the images. false means 
    # we can use convert -flatten to layer the images with transparent backgrounds
    # convert is faster but needs RGBA
    crop = os.environ["CROP_TO_BORDER"].lower()
    if crop == "true":
        command_line = montage_command(frame, x_tiles, y_tiles, output_format)
    else:
        command_line = convert_command(frame, output_format)
    
    print("merge task command line: {}".format(command_line))
    return models.TaskAddParameter(
        id=pad_number(task_id, PAD_LEN_ID),
        display_name="frame: {} - merge task".format(frame),
        command_line=os_specific_command_line(command_line),
        constraints=models.TaskConstraints(max_task_retry_count = 2),
        environment_settings=[
            models.EnvironmentSetting("X_TILES", str(x_tiles)),
            models.EnvironmentSetting("Y_TILES", str(y_tiles))
        ],
        depends_on=models.TaskDependencies(task_ids=get_dependent_tasks(depend_start, depend_end)),
        resource_files=get_resource_files(x_tiles, y_tiles, frame),
        output_files=[
            models.OutputFile(
                file_pattern="../stdout.txt",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/logs/frame-{}/merge.stdout.log".format(job_id, pad_number(frame, PAD_LEN_FRAME))
                    )
                ),
                upload_options=models.OutputFileUploadOptions(models.OutputFileUploadCondition.task_completion)
            ),
            models.OutputFile(
                file_pattern="../stderr.txt",
                destination=models.OutputFileDestination(
                    container=models.OutputFileBlobContainerDestination(
                        container_url=output_sas,
                        path="{}/logs/frame-{}/merge.stderr.log".format(job_id, pad_number(frame, PAD_LEN_FRAME))
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


def blender_command(blend_file, optionalParams): 
    """
    Gets the operating system specific blender exe.
    """
    if os.environ["TEMPLATE_OS"].lower() == "linux":
        command = "blender -b \"{}/{}\" -P \"{}/scripts/python-task-manager.py\" -y -t 0 {}"
    else:
        command = "\"%BLENDER_2018_EXEC%\" -b \"{}\\{}\" -P \"{}\\scripts\\python-task-manager.py\" -y -t 0 {}"

    return command.format(
        os_specific_env("AZ_BATCH_JOB_PREP_WORKING_DIR"),
        blend_file,
        os_specific_env("AZ_BATCH_TASK_WORKING_DIR"),
        optionalParams
    )


def os_specific_env(env_var_name): 
    """
    Gets the operating system specific environment variable format string.

    :param env_var_name: Environment variable name.
    :type env_var_name: str
    """
    current_os = os.environ["TEMPLATE_OS"]
    if current_os.lower() == "linux":
        return "${}".format(env_var_name)
    else:
        return "%{}%".format(env_var_name)


def os_specific_command_line(command_line): 
    """
    Gets the operating system specific command string.

    :param command_line: command line to execute.
    :type command_line: str
    """
    current_os = os.environ["TEMPLATE_OS"]
    command = "/bin/bash -c '{}'" if current_os.lower() == "linux" else "cmd.exe /c \"{}\""
    return command.format(command_line)


def convert_command(frame, output_format):
    """
    Command for executing the ImageMagick 'convert' command.
    This command layers the output image tiles on top of one another 
    and then flattens the image. Faster than the montage command, but 
    doesn't work when the ouput channel is not set to have a 
    transparent background.

    :param frame: Current frame number.
    :type frame: int
    :param output_format: Blender output format (PNG, OPEN_EXR, etc).
    :type output_format: str
    """
    command = ""
    current_os = os.environ["TEMPLATE_OS"]

    if current_os.lower() == "linux":
        command = "cd $AZ_BATCH_TASK_WORKING_DIR;convert tile_* -flatten frame_{}.{}"
    else:
        command = "cd /d %AZ_BATCH_TASK_WORKING_DIR% & magick convert tile_* -flatten frame_{}.{}"
    
    return command.format(
        pad_number(frame, PAD_LEN_FRAME),
        get_file_extension(output_format)
    )


def montage_command(frame, x_tiles, y_tiles, output_format):
    """
    Command for executing the ImageMagick 'montage' command.
    This command tiles the images back together into an X by Y grid.
    Slower than the convert command above, but needed when the ouput 
    channel is not set to have a transparent background.

    :param frame: Current frame number.
    :type frame: int
    :param x_tiles: Number of tiles on the X axis.
    :type x_tiles: int
    :param y_tiles: Number of tiles on the Y axis.
    :type y_tiles: int
    """
    command = ""
    tiles = get_tile_names(x_tiles * y_tiles)
    current_os = os.environ["TEMPLATE_OS"]

    if current_os.lower() == "linux":
        command = "cd $AZ_BATCH_TASK_WORKING_DIR;montage {} -tile {}x{} -background none -geometry +0+0 frame_{}.{}"
    else:
        command = "cd /d %AZ_BATCH_TASK_WORKING_DIR% & magick montage {} -tile {}x{} -background none -geometry +0+0 frame_{}.{}"
    
    return command.format(
        " ".join(tiles),
        x_tiles,
        y_tiles,
        pad_number(frame, PAD_LEN_FRAME),
        get_file_extension(output_format)
    )


def pad_number(number, len=6):
    """
    Used for padding task id's. Takes a number and adds padding zeros
    to it. E.g. 4 => 000004 with default of length 6. 

    :param number: Number to pad.
    :type number: int
    :param len: Total length of the resulting string.
    :type len: int
    """
    return str(number).zfill(len)


def get_file_extension(blender_output_format): 
    """
    Given the Blender output format string, return the associated file
    extension. E.g. PNG => png, OPEN_EXR => exr. 

    :param blender_output_format: Blender output format. Values include: BMP,
     IRIS, PNG, JPEG, JPEG2000, TARGA, TARGA_RAW, CINEON, DPX, OPEN_EXR_MULTILAYER,
     OPEN_EXR, HDR, TIFF. Though we are only interested in a few of these.
    :type blender_output_format: string
    """
    # just png for now
    return blender_output_format.lower()


def get_resource_files(x_tiles, y_tiles, frame):
    """
    Generate a list of resource files for the merge task. These will be the 
    output tiles for the frame. 

    :param x_tiles: Number of tiles on the X axis.
    :type x_tiles: int
    :param y_tiles: Number of tiles on the Y axis.
    :type y_tiles: int
    :param frame: Current frame
    :type frame: int
    """
    tile_count = x_tiles * y_tiles
    output_sas = os.environ["OUTPUT_CONTAINER_SAS"]
    job_id = os.environ["AZ_BATCH_JOB_ID"]
    sas_parts = output_sas.split("?")
    files = []

    for tile_name in get_tile_names(tile_count):
        path_to_file = "{}/outputs/frame-{}/{}".format(job_id, pad_number(frame, PAD_LEN_FRAME), tile_name)
        files.append(models.ResourceFile("{}/{}?{}".format(sas_parts[0], path_to_file, sas_parts[1]), tile_name))

    return files


def get_tile_names(tile_count): 
    """
    Returns an array of output tile names given the count we expect. 
    A 2 x 2 grid will have 4 images: tile_0001 ... tile_0004.

    :param tile_count: Number of tiles we expect the frame to have.
     `os.environ["X_TILES"]` multiplied by `os.environ["Y_TILES"]`.
    :type tile_count: int
    """
    tiles = []
    extension = get_file_extension(os.environ["OUTPUT_FORMAT"])
    for num in range(1, tile_count + 1):
        tiles.append("tile_{}.{}".format(str(num).zfill(3), extension))
    
    return tiles


def get_dependent_tasks(depend_start, depend_end): 
    """
    Returns an array of task IDs for the dependency list. 
    E.g. [000001, 000002, 000003, 000004, ...]

    :param depend_start: range start task ID.
    :type depend_start: int
    :param depend_end: range end task ID.
    :type depend_end: int
    """
    taskIds = []
    for task_id in range(depend_start, depend_end + 1):
        taskIds.append(pad_number(task_id, PAD_LEN_ID))
    
    return taskIds


def submit_task_collection(batch_client, job_id, tasks, frame):
    """
    Submit the colleciton of tasks to the Batch REST service for the 
    given frame. Note that a maximum of 100 tasks can be submitted to the 
    Batch API at any one time.

    :param batch_client: Client for issuing REST requests to the Azure Batch
     service.
    :type batch_client: ~BatchServiceClient
    :param job_id: Identifier of the job that we will be submitting these
     tasks to.
    :type job_id: str
    :param tasks: Collection of tasks to submit to the Batch service.
    :type tasks: list[~models.TaskAddParameter]
    :param frame: Current frame
    :type frame: int
    """
    print("submitting: {} tasks to job: {}, for frame: {}".format(str(len(tasks)), job_id, frame))

    try:
        # split task array into chunks of 100 tasks if the array is larger than
        # 100 items. this is the maximum number of tasks supported by add_collection
        failed = 0
        for chunk in list(chunks(tasks, 100)):
            print("submitting: {} tasks to the Batch service".format(len(chunk)))
            result = batch_client.task.add_collection(job_id=job_id, value=chunk)

            # check there were no failures in the response as add_collection always succeeds
            for taskAddResult in result.value:
                if taskAddResult.status != models.TaskAddStatus.success:
                    failed += 1
                    print("failed to add task: {}, status: {}, error: {}".format(taskAddResult.task_id, taskAddResult.status, str(taskAddResult.error)))
                    for errorDetail in taskAddResult.error.values:
                        print("detail: {}".format(str(errorDetail)))

        if failed != 0:    
            raise Exception("Failed to add all tasks to the Batch service. Check the stdout log file for details.")
    except models.BatchErrorException as bex:
        print("got an error adding tasks: {}".format(str(bex)))
        for errorDetail in bex.inner_exception.values:
            print("detail: {}".format(str(errorDetail)))

        raise bex


def chunks(items, count):
    """
    Split a collection of potentially more than 100 items into a list 
    of lists that contain 100 items. E.g. list[1..150] will return a 
    list[list[1..100], list[101..150]].

    :param items: Collection of items.
    :type items: list[~object]
    :param count: Maximum number of items in any one list.
    :type count: int
    """
    # For item i in a range that is a length of items[],
    for i in range(0, len(items), count):
        # Create an index range for l of n items:
        yield items[i:i+count]


if __name__ == '__main__':
    main()
