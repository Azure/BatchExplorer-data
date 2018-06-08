#!/bin/bash
if [ "$EUID" -ne 0 ]
  then echo "Please run as root"
  exit 1
fi

set -e

BLOBFUSE_CONFIG_FILE="$AZ_BATCH_JOB_PREP_WORKING_DIR/connection.cfg"
MOUNT_POINT_PATH="$AZ_BATCH_JOB_PREP_WORKING_DIR/$INPUT_FILEGROUP_NAME"
TMP_CACHE_PATH="$AZ_BATCH_JOB_PREP_WORKING_DIR/blobfusetmp"
# Get the account name from the SAS URL
SAS_STRING=$(echo $INPUT_FILEGROUP_SAS | rev | cut -d '?' -f 1 | rev)
ACCOUNT_NAME="$(echo $INPUT_FILEGROUP_SAS | cut -d'/' -f3 | cut -d'.' -f1)"
echo "accountName $ACCOUNT_NAME" > $BLOBFUSE_CONFIG_FILE
echo "sasToken ?$SAS_STRING" >> $BLOBFUSE_CONFIG_FILE
echo "containerName $INPUT_FILEGROUP_NAME" >> $BLOBFUSE_CONFIG_FILE

# Configure the apt repository for Microsoft products following: 
# Install repository configuration
curl https://packages.microsoft.com/config/rhel/7/prod.repo > ./microsoft-prod.repo
cp ./microsoft-prod.repo /etc/yum.repos.d/

# Install Microsoft's GPG public key
curl https://packages.microsoft.com/keys/microsoft.asc > ./microsoft.asc
rpm --import ./microsoft.asc

# Install blobfuse
yum install blobfuse fuse -y --nogpgcheck

# Configuring and Running.
if [[ ! -d $TMP_CACHE_PATH ]]; then
    mkdir $TMP_CACHE_PATH
else
    echo "$TMP_CACHE_PATH already exists."
fi

# This is folder naming with the filegroup name.
if [[ ! -d $MOUNT_POINT_PATH ]]; then
    mkdir $MOUNT_POINT_PATH
else
    echo "$MOUNT_POINT_PATH already exists."
fi

# Below is the config file approach
blobfuse "$MOUNT_POINT_PATH" --tmp-path=$TMP_CACHE_PATH -o attr_timeout=240 -o entry_timeout=240 -o negative_timeout=120 --config-file="$BLOBFUSE_CONFIG_FILE"

ls "$MOUNT_POINT_PATH"
sleep 30s
