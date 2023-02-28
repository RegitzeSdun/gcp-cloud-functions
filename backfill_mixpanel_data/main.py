import gzip
import json
import os
import requests
import time
import hashlib

from google.cloud import bigquery
from google.api_core.future import polling
from google.cloud.bigquery.retry import DEFAULT_RETRY
from requests.auth import HTTPBasicAuth

def create_insert_id(original_timestamp, user_id):
    # Mixpanel recommends computing a hash of some set of properties that make the event semantically unique (eg. distinct_id + timestamp + some other property) and using the first 36 characters of that hash as insert id
    unique_str = "Viewed Article-"+str(original_timestamp) + str(user_id)
    return hashlib.sha256(unique_str.encode()).hexdigest()[0:36]

def main(data, context):

    big_query_table_id = "project_id.dbt_rsdun.articles_viewed_backfill_fall_2022"

    # Clean big query table
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../ingest_downloads_to_mixpanel/credentials_file.json"
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials_file.json"
    client = bigquery.Client()
    query = f"""SELECT slug, original_timestamp, category, user_id, referrer, clinic_id, article_id FROM `{big_query_table_id}`;"""
    query_job = client.query(query, retry=DEFAULT_RETRY)
    query_job._retry = polling.DEFAULT_RETRY
    if query_job._result_set:
        print("Succesfully queried Bigquery and got a result set")

    # mixpanel

    
    PROJECT_ID = 'project_id'
    CLIENT_ID = 'client_id'
    CLIENT_SECRET = 'client_secret'

    url = f"https://api.mixpanel.com/import?strict=1&project_id={PROJECT_ID}"

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
            "event": "Viewed Article",
            "properties": {
                "time": int(time.mktime(i['original_timestamp'].timetuple()))*1000,
                "distinct_id": i['user_id'],
                "$insert_id": create_insert_id(i['original_timestamp'], i['user_id']),
                "slug": i['slug'],
                "category": i['category'],
                "referrer": i['referrer'],
                "clinic_id": i['clinic_id'],
                "article_id": i['article_id'],
            }
        })

    print("Created payload")

    response = requests.post(url, data=gzip.compress(json.dumps(payload).encode('utf-8')), headers=headers, auth=HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET))

    print(f"Performed response with following status: {response.status_code} and message: {response.text}")

if __name__ == '__main__':
    main('data', 'context')
    