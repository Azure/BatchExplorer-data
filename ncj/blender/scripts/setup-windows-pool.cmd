echo # installing python ...
if exist "C:\Python37\python.exe" (
    echo # python already installed
) else (
    choco install python -y 
    echo # refreshing environment vars ...
    call RefreshEnv.cmd
    echo # installing azure-batch sdk for python ...
)
choco install microsoft-visual-cpp-build-tools -y
pip install azure-batch==4.1.3
echo Exit Code is %errorlevel%
exit /b %errorlevel%
