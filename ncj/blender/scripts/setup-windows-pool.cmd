echo # installing python ...
if exist "C:\Python37\python.exe" (
    echo # python already installed
    echo # installing microsoft build tools for the azure-batch cli
    choco install microsoft-visual-cpp-build-tools -y
) else (
    choco install python -y 
    echo # refreshing environment vars ...
    call RefreshEnv.cmd
    echo # installing azure-batch sdk for python ...
)
pip install azure-batch==4.1.3
echo Exit Code is %errorlevel%
exit /b %errorlevel%
