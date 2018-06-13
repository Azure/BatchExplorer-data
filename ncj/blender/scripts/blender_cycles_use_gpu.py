import bpy

for scene in bpy.data.scenes:
    scene.cycles.device = 'GPU'

bpy.context.user_preferences.addons['cycles'].preferences.compute_device_type = 'CUDA'

for d in bpy.context.user_preferences.addons['cycles'].preferences.devices:
    if d.type == 'CPU':
        d.use = False
    print("Device '{}' type {} : {}" . format(d.name, d.type, d.use))
