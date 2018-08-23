@echo off
IF %4=="VRayAdv" (
    IF %2=="3ds Max 2018" ( 
	start cmd /c C:\Autodesk\3dsMax2018\vrayspawner2018.exe -port=%6 
    ) 
    IF %2=="3ds Max 2019" ( 
	start cmd /c C:\Autodesk\3dsMax2019\vrayspawner2019.exe -port=%6
    )
)
IF %4=="VRayRT" (
    IF %2=="3ds Max 2018" ( 
	start cmd /c C:\server\3dsMax2018Vray\bin\vray.exe -server -portNumber=%6
    ) 
    IF %2=="3ds Max 2019" ( 
	start cmd /c C:\server\3dsMax2019Vray\bin\vray.exe -server -portNumber=%6  
    )
) 
echo sleeping for 30 seconds 
ping 127.0.0.1 -n 30 > nul
