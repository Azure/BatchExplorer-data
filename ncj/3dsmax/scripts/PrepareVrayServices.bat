set MAX_VERSION=%1
set VRAY_RENDERER=%2
set VRAY_PORT=%3

setx AZ_BATCH_ACCOUNT_URL %AZ_BATCH_ACCOUNT_URL% /M
setx AZ_BATCH_SOFTWARE_ENTITLEMENT_TOKEN %AZ_BATCH_SOFTWARE_ENTITLEMENT_TOKEN% /M

IF "%VRAY_RENDERER%"=="VRayAdv" (

    rem We need to create the config files first
    echo.[Directories] > C:\Autodesk\3dsMax2018\vrayspawner.ini
    echo.AppName=C:\Autodesk\3dsMax2018\3dsmaxio.exe >> C:\Autodesk\3dsMax2018\vrayspawner.ini
    echo.[Directories] > C:\Autodesk\3dsMax2019\vrayspawner.ini
    echo.AppName=C:\Autodesk\3dsMax2019\3dsmaxio.exe >> C:\Autodesk\3dsMax2019\vrayspawner.ini

    IF "%MAX_VERSION%"=="2018" (
	   start /wait "vrayspawner2018" "C:\Autodesk\3dsMax2018\vrayspawner2018.exe" "-port=%VRAY_PORT%"
    ) 
    IF "%MAX_VERSION%"=="2019" (
	   start /wait "vrayspawner2019" "C:\Autodesk\3dsMax2019\vrayspawner2019.exe" "-port=%VRAY_PORT%"
    )
)

IF "%VRAY_RENDERER%"=="VRayRT" (
    IF "%MAX_VERSION%"=="2018" (
	   start /wait "vray2018" "C:\server\3dsMax2018Vray\bin\vray.exe" "-server" "-portNumber=%VRAY_PORT%"
    ) 
    IF "%MAX_VERSION%"=="2019" (
	   start /wait "vray2019" "C:\server\3dsMax2019Vray\bin\vray.exe" "-server" "-portNumber=%VRAY_PORT%"
    )
) 

exit /b %errorlevel%
