import pandas as pd
import json
import gzip
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import requests
import os
import tempfile
#import os, psutil
from google.cloud import bigquery

'''
gcloud functions deploy mixpanel_importer --region=europe-west1 --memory=1024MB --entry-point main --runtime python39 --trigger-resource mixpanel --trigger-event google.pubsub.topic.publish --timeout 540s
'''

def main(data, context):

    tmpdir = tempfile.gettempdir()

    big_query_table_id = "project_id.raw_mixpanel__events.raw_mixpanel__events"

    start_date = datetime(2021,11,1)
    end_date_list = pd.date_range(datetime(2021,12,1),date.today(), freq='MS')

    headers = {
        "accept": "text/plain",
        "authorization": "Basic Z2NwLWltcG9ydC5kNmM3YjUubXAtc2VydmljZS1hY2NvdW50OjNRSXBDRlhpTHh3eXRtWjZjVTZjTTM5UWpVelF5ZzBn"
    }

    # Load schema for mixpanel table
    json_schema_file = open('schema.json')
    json_schema = json.load(json_schema_file)

    # Clean big query table
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials_file.json"
    client = bigquery.Client()
    query = f"""DELETE FROM `{big_query_table_id}` WHERE true;"""
    query_job = client.query(query)
    print(query_job.result())

    for end_date in end_date_list:
        start_date_str = str(start_date.date())
        end_date_str = str(end_date.date())

        url = f"https://data-eu.mixpanel.com/api/2.0/export?project_id=2571509&from_date={start_date_str}&to_date={end_date_str}"
        response = requests.get(url, headers=headers)

        print(f"Request for the period {start_date_str} to {end_date_str} ended with response code {response.status_code}")
        if response.status_code != 200:
            print(response.reason)
            print(response.text)
        
        file_path_gz = tmpdir+f'/events_from_{start_date_str}_to_{end_date_str}.gz'

        with gzip.open(file_path_gz, 'wb') as f:
            f.write(response.content)


        del response

        tmp_df = pd.read_json(file_path_gz, lines=True, compression='gzip')
        tmp_df['properties'] = tmp_df['properties'].apply(json.dumps)

        file_path_csv = tmpdir+f'/events_from_{start_date_str}_to_{end_date_str}.csv'
        tmp_df.to_csv(file_path_csv,index=False)

        del tmp_df

        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV,
                                            skip_leading_rows=1, 
                                            autodetect=False,
                                            schema=json_schema,
                                            write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

        print(file_path_csv)
        with open(file_path_csv, "rb") as source_file:
            job = client.load_table_from_file(source_file,big_query_table_id,job_config=job_config)

        print(job.result())

        start_date += relativedelta(months=+1)
        
        #process = psutil.Process(os.getpid())
        #print(process.memory_info().rss)

if __name__ == '__main__':
    main('data', 'context')