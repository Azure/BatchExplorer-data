
RC_PATH="/mnt/connection.cfg"
INPUT_FILEGROUP_NAME="$1"
INPUT_FILEGROUP_SAS="$2"
# Get the account name from the SAS URL
theAccountName="$(echo $INPUT_FILEGROUP_SAS | cut -d'/' -f3 | cut -d'.' -f1)"
sudo echo "accountName $theAccountName" > $RC_PATH
sudo echo "sasToken $INPUT_FILEGROUP_SAS" >> $RC_PATH
sudo echo "containerName $INPUT_FILEGROUP_NAME" >> $RC_PATH
# Configure the apt repository for Microsoft products following: 
# Install repository configuration
sudo curl https://packages.microsoft.com/config/rhel/7/prod.repo > ./microsoft-prod.repo
sudo cp ./microsoft-prod.repo /etc/yum.repos.d/
# Install Microsoft's GPG public key
sudo curl https://packages.microsoft.com/keys/microsoft.asc > ./microsoft.asc
sudo rpm --import ./microsoft.asc
# Install blobfuse
sudo yum install blobfuse fuse -y
# Configuring and Running.
sudo mkdir /mnt/blobfusetmp
# This is temporary naming for testing
sudo mkdir /mnt/blobfusetest
# Below is the config file approach
blobfuse /mnt/blobfusetest --tmp-path=/mnt/blobfusetmp -o attr_timeout=240 -o entry_timeout=240 -o negative_timeout=120 --config-file="$RC_PATH"