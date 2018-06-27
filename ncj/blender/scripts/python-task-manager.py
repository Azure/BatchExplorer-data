import bpy
import os
import sys

PAD_LEN_TILE = 3

def main():
    try:
        print("-------------------------------")
        print("task manager reporting for duty")
        print("-------------------------------")

        job_id = os.environ["AZ_BATCH_JOB_ID"]
        current_tile = int(os.environ["CURRENT_TILE"])
        batch_account_url = os.environ["AZ_BATCH_ACCOUNT_URL"]
        task_id  = os.environ["AZ_BATCH_TASK_ID"]
        
        print("blender with file: {}".format(bpy.data.filepath))
        print("job_id: {}, task_id: {}, batch_account_url: {}".format(job_id, task_id, batch_account_url))
        sys.stdout.flush()

        render_tile(current_tile)
    except Exception as ex:
        print("Failed to render tile: {}, with error: {}".format(current_tile, ex))
        raise ex


def render_tile(current_tile):
    x_tiles = int(os.environ["X_TILES"])
    y_tiles = int(os.environ["Y_TILES"])
    current_x = int(os.environ["CURRENT_X"])
    current_y = int(os.environ["CURRENT_Y"])
    print("x_tiles: {}, y_tiles: {}, current_x: {}, current_y: {}, tile: {}".format(x_tiles, y_tiles, current_x, current_y, current_tile))

    # Set current frame
    current_frame = int(os.environ["CURRENT_FRAME"])
    print("setting current frame to: {}".format(current_frame))
    bpy.context.scene.frame_current = current_frame

    # RGB or RGBA
    color_mode = bpy.context.scene.render.image_settings.color_mode
    print("color_mode: {}".format(color_mode))

    # Output format
    output_format = os.environ["OUTPUT_FORMAT"]
    print("current file format: {}".format(bpy.context.scene.render.image_settings.file_format))
    bpy.context.scene.render.image_settings.file_format = output_format
    file_format = bpy.context.scene.render.image_settings.file_format
    if file_format in ("OPEN_EXR", "OPEN_EXR_MULTILAYER"):
        file_format = "exr"

    print("file format prior to render: {}".format(file_format))

    # Render engine, maybe add this to job teamplate
    # Options - BLENDER_RENDER, BLENDER_GAME, CYCLES
    print("render engine: {}".format(bpy.context.scene.render.engine))

    # Compute the tiles border
    total_tiles = x_tiles * y_tiles
    min_x = current_x / x_tiles
    max_x = (current_x + 1) / x_tiles
    min_y = 1 - (current_y + 1) / y_tiles
    max_y = 1 - current_y / y_tiles

    print("rendering on host: {}".format(os.environ["AZ_BATCH_NODE_ID"]))
    print("rendering tile '{}' of '{}' - min_x: {}, max_x: {} || min_y: {}, max_y: {}".format(current_tile, total_tiles, min_x, max_x, min_y, max_y))
    sys.stdout.flush()

    # use border render and set the coordinates
    bpy.context.scene.render.use_border = True

    # check if we want to crop-to-border. crop-to-border set to false means the tile is the 
    # same size as the final output image, but only the tile border is rendered. True means that the 
    # tile is only the size of the tile itself with no background. Depending on this setting is how
    # we merge the tiles back together again.
    crop = os.environ["CROP_TO_BORDER"].lower()
    bpy.context.scene.render.use_crop_to_border = True if crop == "true" else False
    print("use_crop_to_border: {}".format(crop))

    # set border values for the render box
    bpy.context.scene.render.border_min_x = min_x
    bpy.context.scene.render.border_max_x = max_x
    bpy.context.scene.render.border_min_y = min_y
    bpy.context.scene.render.border_max_y = max_y

    # kick off the render
    bpy.ops.render.render()
    
    # todo: get extension from scene
    directory = os.environ["AZ_BATCH_TASK_WORKING_DIR"]
    save_path = "{}/tile_{}.{}".format(directory, str(current_tile).zfill(PAD_LEN_TILE), file_format.lower())

    print("Saving to: {}".format(save_path)) 
    sys.stdout.flush()

    bpy.data.images["Render Result"].save_render(save_path)


if __name__ == '__main__':
    main()
