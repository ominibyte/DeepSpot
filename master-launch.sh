#!/bin/bash

# Install python
sudo yum -y install gcc openssl-devel bzip2-devel libffi-devel
cd /opt
sudo wget https://www.python.org/ftp/python/3.7.4/Python-3.7.4.tgz
sudo tar xzf Python-3.7.4.tgz
cd Python-3.7.4
sudo ./configure --enable-optimizations
sudo make altinstall

# Configure awscli
aws configure set aws_access_key_id <aws_access_key_id>
aws configure set aws_secret_access_key <aws_secret_access_key>
aws configure set default.region ca-central-1
aws configure set default.output json

# Create a directory in the home folder
mkdir ~/deepspot

# Change the working directory
cd ~/deepspot

# Download the slave script to the directory
curl https://comp598-deepspot.s3.ca-central-1.amazonaws.com/master.exe --output master.exe


# Make sure the file is executable
chmod +x master.exe

# Run the startup script
./master.exe
