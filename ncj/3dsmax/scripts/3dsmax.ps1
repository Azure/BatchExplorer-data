param (
    [int]$start = 1,
    [int]$end = 1,
    [string]$outputName = "images\image.jpg",
    [int]$width = 800,
    [int]$height = 600,
    [string]$sceneFile,
    [int]$nodeCount = 1,
    [switch]$dr,
    [string]$renderer = "vray",
    [string]$irradianceMap = $null,
    [string]$pathFile = $null,
    [string]$workingDirectory = "$env:AZ_BATCH_JOB_PREP_WORKING_DIR\assets",
    [string]$preRenderScript = $null,
    [string]$camera = $null,
    [string]$additionalArgs = $null,
    [int]$vrayPort = 20204,
    [string]$renderPresetFile = $null
)

$OutputEncoding = New-Object -typename System.Text.UnicodeEncoding

function ParameterValueSet([string]$value)
{
    return ($value -and -Not ($value -eq "none") -and -Not ([string]::IsNullOrWhiteSpace($value)))
}

function SetupDistributedRendering
{
    Write-Host "Setting up DR..."

    $port = $vrayPort
    $vraydr_file = "vray_dr.cfg"
    $vrayrtdr_file = "vrayrt_dr.cfg"
    $hosts = $env:AZ_BATCH_HOST_LIST.Split(",")

    if ($hosts.Count -ne $nodeCount) {
        Write-Host "Host count $hosts.Count must equal nodeCount $nodeCount"
        exit 1
    }

    $env:AZ_BATCH_HOST_LIST.Split(",") | ForEach {
        "$_ 1 $port" | Out-File -Append $vraydr_file
        "$_ 1 $port" | Out-File -Append $vrayrtdr_file
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

@"
autostart_local_slave 0
"@ | Out-File -Append $vrayrtdr_file

    New-Item "$env:LOCALAPPDATA\Autodesk\3dsMaxIO\2018 - 64bit\ENU\en-US\plugcfg" -ItemType Directory
    cp $vraydr_file "$env:LOCALAPPDATA\Autodesk\3dsMaxIO\2018 - 64bit\ENU\en-US\plugcfg\vray_dr.cfg"
    cp $vrayrtdr_file "$env:LOCALAPPDATA\Autodesk\3dsMaxIO\2018 - 64bit\ENU\en-US\plugcfg\vrayrt_dr.cfg"

# Create preRender script to enable distributed rendering in the scene
@"
-- Enables VRay DR
-- The VRay RT and VRay Advanced renderer have different DR properties
-- so we need to detect the renderer and use the appropriate one.
r = renderers.current
rendererName = r as string
index = findString rendererName "V_Ray_"
if index != 1 then (print "VRay renderer not used, please save the scene with a VRay renderer selected.")
index = findString rendererName "V_Ray_RT_"
if index == 1 then (r.distributed_rendering = true) else (r.system_distributedRender = true;r.system_vrayLog_level = 4; r.system_vrayLog_file = "%AZ_BATCH_TASK_WORKING_DIR%\VRayLog.log")
"@ | Out-File -Append $pre_render_script
}

# Create pre-render script
$pre_render_script = "prerender.ms"
@"
-- Pre render script
r = renderers.current
"@ | Out-File $pre_render_script

if ($dr)
{
    SetupDistributedRendering
}

if (ParameterValueSet $irradianceMap -and $renderer -eq "vray")
{
    $irMap = "$workingDirectory\$irradianceMap"
    Write-Host "Setting IR map to $irMap"
@"
-- Set the IR path
r.adv_irradmap_loadFileName = "$irMap"
"@ | Out-File -Append $pre_render_script
}

if ($renderer -eq "arnold")
{
@"
-- Fail on arnold license error
r.abort_on_license_fail = true
"@ | Out-File -Append $pre_render_script
}

if ($renderer -eq "vray")
{
    $outputFiles = "$env:AZ_BATCH_TASK_WORKING_DIR\images\____.jpg" -replace "\\", "\\"
@"
-- Set output channel path
r.output_splitfilename = "$outputFiles"
"@ | Out-File -Append $pre_render_script
}

if (ParameterValueSet $preRenderScript)
{
    $preRenderScript = "$workingDirectory\$preRenderScript"
    
    if (-Not [System.IO.File]::Exists($preRenderScript))
    {        
        Write-Host "Pre-render script $preRenderScript not found, exiting."
        exit 1
    }

    "`r`n" | Out-File -Append $pre_render_script
    Get-Content -Path $preRenderScript | Out-File -Append $pre_render_script
}
else
{
    Write-Host "No pre-render script specified"
}

$sceneFile = "$workingDirectory\$sceneFile"
Write-Host "Using absolute scene file $sceneFile"

$pathFileParam = ""
if (ParameterValueSet $pathFile)
{
    $pathFile = "$workingDirectory\$pathFile"

    if (-Not [System.IO.File]::Exists($pathFile))
    {        
        Write-Host "Path file $pathFile not found, exiting."
        exit 1
    }

    Write-Host "Using path file $pathFile"
    
    # If we're using a path file we need to ensure the scene file is located at the same
    # location otherwise 3ds Max 2018 IO has issues finding textures.
    $sceneFileName = [System.IO.Path]::GetFileName($sceneFile)
    $sceneFileDirectory = [System.IO.Path]::GetDirectoryName("$sceneFile")
    $pathFileDirectory = [System.IO.Path]::GetDirectoryName($pathFile)
    if ($sceneFileDirectory -ne $pathFileDirectory)
    {
        Write-Host "Moving scene file to $pathFileDirectory"
        Move-Item -Force "$sceneFile" "$pathFileDirectory" -ErrorAction Stop > $null
        $sceneFile = "$pathFileDirectory\$sceneFileName"
    }
    $pathFileParam = "-pathFile:`"$pathFile`""
}
else
{
    Write-Host "No path file specified"
}

$cameraParam = ""
if (ParameterValueSet $camera)
{
    Write-Host "Using camera $camera"
    $cameraParam = "-camera:`"$camera`""
}
else
{
    Write-Host "No camera specified"
}

$additionalArgumentsParam = ""
if (ParameterValueSet $additionalArgs)
{
    Write-Host "Using additional arguments $additionalArgs"
    $additionalArgumentsParam = $additionalArgs
}

$renderPresetFileParam = ""
if (ParameterValueSet $renderPresetFile)
{
    $renderPresetFile = "$workingDirectory\$renderPresetFile"

    if (-Not [System.IO.File]::Exists($renderPresetFile))
    {
        Write-Host "Render preset file $renderPresetFile not found, exiting."
        exit 1
    }

    $renderPresetFileParam = "-preset:`"$renderPresetFile`""
}

# Create folder for outputs
mkdir -Force images > $null

# Render
$max_exec = "3dsmaxcmdio.exe"
if ($env:3DSMAX_2018 -and (Test-Path "$env:3DSMAX_2018"))
{
    # New image
    $max_exec = "${env:3DSMAX_2018}\3dsmaxcmdio.exe"
}

Write-Host "Executing $max_exec -secure off -v:5 -rfw:0 $cameraParam $renderPresetFileParam $additionalArgumentsParam -preRenderScript:`"$pre_render_script`" -start:$start -end:$end -outputName:`"$outputName`" -width:$width -height:$height $pathFileParam `"$sceneFile`""

cmd.exe /c $max_exec -secure off -v:5 -rfw:0 $cameraParam $renderPresetFileParam $additionalArgumentsParam -preRenderScript:`"$pre_render_script`" -start:$start -end:$end -outputName:`"$outputName`" -width:$width -height:$height $pathFileParam `"$sceneFile`" `>Max_frame.log 2`>`&1
$result = $lastexitcode

Copy-Item "$env:LOCALAPPDATA\Autodesk\3dsMaxIO\2018 - 64bit\ENU\Network\Max.log" .\Max_full.log -ErrorAction SilentlyContinue

if ($renderer -eq "vray")
{
    Copy-Item "$env:LOCALAPPDATA\Temp\vraylog.txt" . -ErrorAction SilentlyContinue
}

exit $result
