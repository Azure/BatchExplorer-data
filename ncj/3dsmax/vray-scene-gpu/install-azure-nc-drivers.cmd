set driver_version=385.08
set driver_filename=%driver_version%-tesla-desktop-winserver2016-international-whql.exe

rem If already installed, skip
if exist init.txt exit /b 0

rem Install Chocolatey - https://chocolatey.org 
@"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -InputFormat None -ExecutionPolicy Bypass -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))" && SET "PATH=%PATH%;%ALLUSERSPROFILE%\chocolatey\bin"
if %errorlevel% neq 0 exit /b %errorlevel%

rem Install 7zip
choco install -y 7zip
if %errorlevel% neq 0 exit /b %errorlevel%

rem Download NVIDIA Tesla/CUDA drivers
powershell.exe Invoke-WebRequest -Uri "http://us.download.nvidia.com/Windows/Quadro_Certified/%driver_version%/%driver_filename%" -OutFile "%driver_filename%"
if %errorlevel% neq 0 exit /b %errorlevel%

rem Extract and install NVIDIA drivers
7z x -y %driver_filename%
if %errorlevel% neq 0 exit /b %errorlevel%

setup.exe -s
if %errorlevel% neq 0 exit /b %errorlevel%

rem Write a flag so we know we're done
echo done > init.txt

rem Initiate a reboot of the VM
start shutdown /r /t 5
