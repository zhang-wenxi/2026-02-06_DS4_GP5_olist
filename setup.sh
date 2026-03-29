#!/bin/bash

# 1. Environment Setup
echo "--- Setting up Conda Environment ---"
conda env update --file environment.yml --prune
# Note: User must manually run 'conda activate olist-bq' after this script 
# or ensure the terminal is already in the environment.

# 2. dbt Profile Setup (from your history line 82)
echo "--- Configuring dbt Profiles ---"
mkdir -p ~/.dbt
cp dbt_olist/profiles.yml ~/.dbt/profiles.yml

# 3. Credentials & GCP (from your history line 88)
echo "--- Authenticating with Google Cloud ---"
gcloud auth application-default login

# 4. Ingestion (Meltano)
echo "--- Running Meltano Ingestion ---"
# This uses your logic to load .env into memory (from line 117)
export $(cat .env | grep -v '#' | xargs)
meltano --cwd meltano run tap-csv target-bigquery

# 5. dbt Initialization (from your history lines 110-111)
echo "--- Installing dbt dependencies and parsing ---"
cd dbt_olist
dbt deps
dbt parse --no-partial-parse
dbt build
dbt docs generate
cd ..

# 6. Launch Dagster (from your history line 173/178)
echo "--- Starting Dagster UI ---"
export DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1
export $(cat .env | grep -v '#' | xargs)
dagster dev -f dagster/definition.py
