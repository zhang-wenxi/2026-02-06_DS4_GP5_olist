#!/bin/bash

# 1. Activate Environment (Friend must do this first in terminal)
# conda activate olist-bq

# 2. Load Credentials into Memory (Crucial for dbt to talk to BigQuery)
echo "--- Loading .env and GCP Auth ---"
export $(cat .env | grep -v '#' | xargs)
gcloud auth application-default login

# 3. Initialize dbt (Based on your history lines 110, 111, 174)
echo "--- Preparing dbt Models ---"
cd dbt_olist
dbt deps           # Installs dbt_expectations (Fixes line 110 error)
dbt parse          # Generates manifest.json for Dagster (Fixes line 111 error)
dbt build          # Runs models and tests (Fixes line 167)
dbt docs generate  # Creates documentation (Fixes line 174)
cd ..

# 4. Launch Dagster (Based on your history line 173)
echo "--- Launching Dagster Orchestration ---"
export DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1
dagster dev -f dagster/definition.py
