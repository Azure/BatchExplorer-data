#!/bin/bash
RC_PATH="$AZ_BATCH_JOB_PREP_WORKING_DIR/connection.cfg"
MOUNT_POINT_PATH="$AZ_BATCH_JOB_PREP_WORKING_DIR/$INPUT_FILEGROUP_NAME"
TMP_CACHE_PATH="$AZ_BATCH_JOB_PREP_WORKING_DIR/blobfusetmp"
# Get the account name from the SAS URL
SAS_STRING=$(echo $INPUT_FILEGROUP_SAS | rev | cut -d '?' -f 1 | rev)
theAccountName="$(echo $INPUT_FILEGROUP_SAS | cut -d'/' -f3 | cut -d'.' -f1)"
sudo echo "accountName $theAccountName" > $RC_PATH
sudo echo "sasToken ?$SAS_STRING" >> $RC_PATH
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
if [[ ! -d $TMP_CACHE_PATH ]]; then
    sudo mkdir $TMP_CACHE_PATH
else
    echo "$TMP_CACHE_PATH already exists."
fi 
# This is folder naming with the filegroup name.
if [[ ! -d $MOUNT_POINT_PATH ]]; then
    sudo mkdir $MOUNT_POINT_PATH
else
    echo "$MOUNT_POINT_PATH already exists."
fi
# Below is the config file approach
blobfuse $MOUNT_POINT_PATH --tmp-path=$MOUNT_POINT_PATH -o attr_timeout=240 -o entry_timeout=240 -o negative_timeout=120 --config-file="$RC_PATH"