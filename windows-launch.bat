:: Create a directory in the home folder
md deepspot

:: Change the working directory
cd deepspot

:: Install wget
pip3 install wget

:: Download the startup script to the directory
python -c "import wget; wget.download('https://comp598-deepspot.s3.ca-central-1.amazonaws.com/startup.exe');"

:: Make sure the file is executable
::chmod +x startup.bat

:: Run the startup script
START startup.exe