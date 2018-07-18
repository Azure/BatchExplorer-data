
echo # installing choco ...
choco install python -y 
echo # installing azure-batch sdk for python ...
pip install azure-batch
echo Exit Code is %errorlevel%
exit /b %errorlevel%
