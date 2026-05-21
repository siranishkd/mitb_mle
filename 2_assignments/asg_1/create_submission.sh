#!/bin/bash

# Create a clean submission zip
echo "Packaging submission zip..."

# Create a temporary directory for staging
mkdir -p submission/temp_stage

# Copy required files
cp main.py submission/temp_stage/
cp Dockerfile submission/temp_stage/
cp docker-compose.yaml submission/temp_stage/
cp requirements.txt submission/temp_stage/
cp -r utils submission/temp_stage/
cp -r data submission/temp_stage/

# Ensure Readme.txt exists
if [ ! -f "Readme.txt" ]; then
    echo "https://github.com/your-username/your-repo-link" > submission/temp_stage/Readme.txt
else
    cp Readme.txt submission/temp_stage/
fi

# Zip the contents
cd submission/temp_stage
zip -r ../assignment1_submission.zip ./*
cd ../..

# Cleanup
rm -rf submission/temp_stage

echo "Submission packaged successfully at submission/assignment1_submission.zip"
