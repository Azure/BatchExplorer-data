
set MAX_VERSION=%1
set VRAY_RENDERER=%2
set VRAY_PORT=%3

rem We need to create the config files first
echo.[Directories] > C:\Autodesk\3dsMax2018\vrayspawner.ini
echo.AppName=C:\Autodesk\3dsMax2018\3dsmaxio.exe >> C:\Autodesk\3dsMax2018\vrayspawner.ini
echo.[Directories] > C:\Autodesk\3dsMax2019\vrayspawner.ini
echo.AppName=C:\Autodesk\3dsMax2019\3dsmaxio.exe >> C:\Autodesk\3dsMax2019\vrayspawner.ini

IF %VRAY_RENDERER%=="VRayAdv" (
    IF %MAX_VERSION%=="2018" ( 
	   start cmd /c C:\Autodesk\3dsMax2018\vrayspawner2018.exe -port=%VRAY_PORT%
    ) 
    IF %MAX_VERSION%=="2019" ( 
	   start cmd /c C:\Autodesk\3dsMax2019\vrayspawner2019.exe -port=%VRAY_PORT%
    )
)

IF %VRAY_RENDERER%=="VRayRT" (
    IF %MAX_VERSION%=="2018" ( 
	   start cmd /c C:\server\3dsMax2018Vray\bin\vray.exe -server -portNumber=%VRAY_PORT%
    ) 
    IF %MAX_VERSION%=="2019" ( 
	   start cmd /c C:\server\3dsMax2019Vray\bin\vray.exe -server -portNumber=%VRAY_PORT% 
    )
) 
echo sleeping for 30 seconds 
ping 127.0.0.1 -n 30 > nul
