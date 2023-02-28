import gzip
import json
import os
import requests
import time

from google.cloud import bigquery
from google.api_core.future import polling
from google.cloud.bigquery.retry import DEFAULT_RETRY
from requests.auth import HTTPBasicAuth


'''
Run this command within this folder after being logged in GCP to deploy the cloud function
gcloud functions deploy downloads_importer_to_mixpanel --region=europe-west1 --entry-point main --runtime python39 --trigger-resource trigger_downloads_import_to_mixpanel --trigger-event google.pubsub.topic.publish --timeout 540s
'''

def main(data, context):

    big_query_table_id = "project_id.analytics.downloads"

    # Clean big query table
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../ingest_downloads_to_mixpanel/credentials_file.json"
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials_file.json"
    client = bigquery.Client()
    query = f"""SELECT download_date, apple_downloads, google_downloads, total_downloads FROM `{big_query_table_id}`;"""
    query_job = client.query(query, retry=DEFAULT_RETRY)
    query_job._retry = polling.DEFAULT_RETRY
    if query_job._result_set:
        print("Succesfully queried Bigquery and got a result set")

    # mixpanel
    PROJECT_ID = 2840017

    url = f"https://api.mixpanel.com/import?strict=1&project_id={PROJECT_ID}"

    CLIENT_ID = 'import-downloads-from-bigquery.1de87d.mp-service-account'
    CLIENT_SECRET = 'pzvCUC9sYs2daEJ06BVVRSyS7zz2MFvu'

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Content-Encoding": "gzip"        
    }
    print("Defined mixpanel timing")

    payload = []

    for i in query_job.result():
        
        payload.append(
            {
            "event": "downloads",
            "properties": {
                "time": int(time.mktime(i['download_date'].timetuple()))*1000,
                "distinct_id": "",
                "$insert_id": "downloads-"+str(i['download_date']),
                "apple_downloads": i['apple_downloads'],
                "google_downloads": i['google_downloads'],
                "total_downloads": i['total_downloads'],
            }
        })

    print("Created payload")

    response = requests.post(url, data=gzip.compress(json.dumps(payload).encode('utf-8')), headers=headers, auth=HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET))

    print(f"Performed response with following status: {response.status_code} and message: {response.text}")

if __name__ == '__main__':
    main('data', 'context')
    