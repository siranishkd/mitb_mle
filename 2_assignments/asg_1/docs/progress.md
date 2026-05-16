# Project Progress: Assignment 1

## Overview
This document tracks the steps taken towards completing Assignment 1, specifically focusing on building the data engineering pipeline.

## Completed Tasks

1. **Requirements Gathering & PDF Extraction:**
   - Extracted text from `Assignment 1.pdf`.
   - Identified core objective: Build a Bronze -> Silver -> Gold Medallion architecture data pipeline for an ML Feature Store.
   - Identified critical requirement: Prevent **Data Leakage**.

2. **Docker Scaffolding:**
   - Created `Dockerfile` mirroring Lab 2 requirements (Python 3.12-slim, Java/PySpark, JupyterLab).
   - Created `docker-compose.yaml` to spin up `asg1_jupyter_lab` and mount the local directory.
   - Created `requirements.txt` with necessary dependencies.
   - *Successfully tested Docker build process.*

3. **Application Scaffolding:**
   - Created `main.py` scaffolding that sets up a PySpark session and automatically creates the target `datamart/bronze`, `datamart/silver`, and `datamart/gold` directories.
   - Created `.gitignore` in `asg_1/` to ensure `data/` and large files are not committed to version control.

4. **Git Version Control:**
   - Switched to a new working branch: `asg_1`.
   - Created `docs/` folder for architectural and progress documentation.

## Next Steps
- Execute `docker-compose up` to run the container.
- Open the Jupyter Lab instance (port 8888).
- Perform Exploratory Data Analysis (EDA) on the 4 CSVs in `data/`.
- Begin implementing the Bronze layer logic inside `main.py` or separate modular utilities.
