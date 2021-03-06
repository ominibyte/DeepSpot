#!/bin/bash

# Activate the TensorFlow virtual environment
source activate tensorflow_p36

# Install awscli
#pip3 install awscli

# Configure awscli
aws configure set aws_access_key_id <aws_access_key_id>
aws configure set aws_secret_access_key <aws_secret_access_key>
aws configure set default.region ca-central-1
aws configure set default.output json

# Create a directory in the home folder
mkdir ~/deepspot

# Change the working directory
cd ~/deepspot

#TODO download the input, model and script
aws s3 cp s3://comp598-deepspot/{JOBID} . --recursive

# Download the slave script to the directory
curl https://comp598-deepspot.s3.ca-central-1.amazonaws.com/slave.exe --output slave.exe

# Make sure the file is executable
chmod +x slave.exe

# Run the startup script
./slave.exe {JOBID}
