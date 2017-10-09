param (
    [int]$start = 1,
    [int]$end = 1,
    [string]$outputName = "images\image.jpg",
    [int]$width = 800,
    [int]$height = 600,
    [string]$sceneFile,
    [int]$nodeCount = 1
)

$port = 20207
$vraydr_file = "vray_dr.cfg"
$pre_render_script = "enable-dr.ms"

$hosts = $env:AZ_BATCH_HOST_LIST.Split(",")

if ($hosts.Count -ne $nodeCount) {
    Write-Host "Host count $hosts.Count must equal nodeCount $nodeCount"
    exit 1
}

$env:AZ_BATCH_HOST_LIST.Split(",") | ForEach {
    "$_ $port" | Out-File -Append $vraydr_file
}

# Create vray_dr.cfg with cluster hosts
@"
restart_slaves 0
list_in_scene 0
max_servers 0
use_local_machine 0
transfer_missing_assets 1
use_cached_assets 1
cache_limit_type 2
cache_limit 100.000000
"@ | Out-File -Append $vraydr_file
New-Item "$env:LOCALAPPDATA\Autodesk\3dsMax\2018 - 64bit\ENU\en-US\plugcfg" -ItemType Directory
New-Item "$env:LOCALAPPDATA\Autodesk\3dsMaxIO\2018 - 64bit\ENU\en-US\plugcfg" -ItemType Directory
cp $vraydr_file "$env:LOCALAPPDATA\Autodesk\3dsMax\2018 - 64bit\ENU\en-US\plugcfg\vray_dr.cfg"
cp $vraydr_file "$env:LOCALAPPDATA\Autodesk\3dsMax\2018 - 64bit\ENU\en-US\plugcfg\vrayrt_dr.cfg"
cp $vraydr_file "$env:LOCALAPPDATA\Autodesk\3dsMaxIO\2018 - 64bit\ENU\en-US\plugcfg\vray_dr.cfg"
cp $vraydr_file "$env:LOCALAPPDATA\Autodesk\3dsMaxIO\2018 - 64bit\ENU\en-US\plugcfg\vrayrt_dr.cfg"

# Create preRender script to enable distributed rendering in the scene
@"
-- Enables VRay DR
-- The VRay RT and VRay Advanced renderer have different DR properties
-- so we need to detect the renderer and use the appropriate one.
vr = renderers.current
rendererName = vr as string
index = findString rendererName "V_Ray_"
if index != 1 then (print "VRay renderer not used, please save the scene with a VRay renderer selected.")
index = findString rendererName "V_Ray_RT_"
if index == 1 then (vr.distributed_rendering = true) else (vr.system_distributedRender = true;vr.system_vrayLog_level = 4; vr.system_vrayLog_file = "%AZ_BATCH_TASK_WORKING_DIR%\VRayLog.txt")
"@ | Out-File $pre_render_script

# Create folder for outputs
mkdir images

# Render
3dsmaxcmdio.exe -secure off -v:5 -rfw:0 -preRenderScript:$pre_render_script -start:$start -end:$end -outputName:"$outputName" -width:$width -height:$height "$sceneFile"

exit $lastexitcode
