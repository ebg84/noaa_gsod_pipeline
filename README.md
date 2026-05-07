# GSOD Lifecycle Pipeline
A meteorological data lifecycle pipeline utilizing National Oceanic and Atmospheric Administration (NOAA) Global Surface Summary of the Day (GSOD) CSV files. 


# Overview
This pipeline ingests, validates, routes, transforms, notifies, stories, and herein archives NOAA GSOD CSV weather data across four US airport stations (Chicago O'Hare, Miami International, Truckee Airport, and Fairbanks International) from the years 2019, 2020, and 2021. Clean and quarantine outputs are produced, with data quality flags included in both clean and quarantine outputs, and failure reasons included in quarantine.


# Pipeline Stages
1. Ingest: Read all 12 GSOD CSV files from Cloud Storage (Chicago, Truckee, Fairbanks, Miami, years 2019-2021

2. Validate: Apply data quality checks, populate validation_flag and quality_flag

3. Route: Split records into clean (for Transformation) and quarantine (straight to Store) based on validation_flag

4. Transform: Apply column renaming, FRSHTT parsing, drop ATTRIBUTES columns

5. Notify: Publish Pub/Sub message summarizing quarantine and quality flag results

6. Store: Write clean/quarantine records to Cloud Storage buckets (eg-gsod-clean from Transform stage and eg-gsod-quarantine from Route stage)

7. Archive: Push pipeline code to GitHub


# Dataset
NOAA Global Surface Summary of the Day
4 Stations, 3 years, ~365 records per file, 4,382 total records
28 total columns, 6 metadata fields, 22 measurement fields
Source: https://www.ncei.noaa.gov/access/search/data-search/global-summary-of-the-day


# Prerequisites
* Python 3.11
* GCP access with Cloud Storage and Pub/Sub enabled
* Compute Engine VM with Cloud APIs enabled
* Pub/Sub topic named 'pipeline-alerts' for notifications


# Installation
Recommended to use a specified virtual environment to avoid dependency issues

```bash
python3 -m venv pipeline-env
source pipeline-env/bin/activate
pip install -r requirements.tct
```


# Data Preprocessing
Before pipeline run, two preprocessing steps are required on any new NOAA GSOD CSV files before ingestion to Cloud Storage. Current files in 'eg-gsod-raw' have transformations applied already

1. File renaming: All GSOD files downloaded from the same station have the same default filename. Append a year suffix to each file (e.g. '72530094846.csv' -> '72530094846_2019.csv') to distinguish year of station file.

2. Station name delimiter replacement: Station names contain a comma separating airport or station name from state and country. A space and then hyphen were added in place of the comma to avoid delimiter issues in CSV parsing (e.g. 'CHICAGO OHARE INTERNATIONAL AIRPORT, IL US' -> 'CHICAGO OHARE INTERNATIONAL AIRPORT - IL US')


# GCP Configuration
Three Cloud Storage buckets are required

* 'eg-gsod-raw' contains raw input of CSVs organized by station subfolders
* 'eg-gsod-quarantine' will contain quarantined records with validation_flag field indicating failure reason
* 'eg-gsod-clean' will contain transformed records with quality_flag field indicating potential data quality issues

```python
PROJECT_ID = "project_id"
RAW_BUCKET = "eg-gsod-raw"
CLEAN_BUCKET = "eg-gsod-clean"
QUARANTINE_BUCKET = "eg-gsod-quarantine"
PUBSUB_TOPIC = "pipeline-alerts"
```


# Pipeline Run
```bash
python3 python_noaa_gsod_pipeline.py
```


# Repository Structure
data/
  eg-gsod-raw/
    chicago/
    fairbanks/
    miami/
    truckee/
  eg-gsod-quarantine/
    quarantine/ (Quarantined records)
  eg-gsod-clean/
    clean/ (Transformed clean outputs)
README.md
python_noaa_gsod_pipeline.py
requirements.txt


# Output
* Clean records: Analysis ready CSV with renamed columns, boolean weather event fields, and quality_flag field for potential data quality issues

* Quarantine records: Original NOAA format with added validation_flag for failure reason and _source_file for file origination


# Teardown
1. Stop or delete Compute Engine VM when pipeline is complete.

2. Delete Cloud Storage bucket contents when analysis is finished or necessary files downloaded

