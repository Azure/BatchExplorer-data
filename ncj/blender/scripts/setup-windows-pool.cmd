
echo # installing choco ...
choco install python yes to all 
refreshenv
echo # installing azure-batch sdk for python ...
pip install azure-batch
refreshenv 
echo Exit Code is %errorlevel%
exit /b %errorlevel%
