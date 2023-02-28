# data-pipeline

## Cloud functions
This repos consists of three cloud functions currently deployed in production:
* ingest_app_store_data_to_bigquery
* ingest_downloads_to_mixpanel
* ingest_mixpanel_data_to_bigquery

Each cloud function has a main file, with the relevant script command to deploy the function.

## Backfills
One time functiomns used to backfill mixpanel data. Kept here so that they can be reused for other backfills.