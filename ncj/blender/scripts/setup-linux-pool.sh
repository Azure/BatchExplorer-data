#!/bin/bash
apt-get update
apt-get install -y imagemagick
apt-get install -y blender
apt-get install -y python-pip
pip install azure-batch
wget -O azcopy.tar.gz https://aka.ms/downloadazcopyprlinux
tar -xf azcopy.tar.gz
./install.sh
exit $?
