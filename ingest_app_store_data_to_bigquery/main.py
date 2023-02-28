"""Function called by PubSub trigger to execute cron job tasks."""
import os
import config
import logging
import tempfile
import json

from datetime import date, timedelta

from appstoreconnect import Api
from google.cloud import bigquery
import pandas as pd

'''
gcloud functions deploy --region=europe-west1 apple_app_store_importer --entry-point main --runtime python39 --trigger-resource app-store-connect --trigger-event google.pubsub.topic.publish --timeout 540s
'''


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def get_last_sync_date(client):
    query = f"SELECT MAX(Begin_Date) FROM {config.config_vars['TABLE_ID']} LIMIT 1000"
    job = client.query(query)

    for row in job:
        if row[0] is None:
            return date(2022, 1, 1)
        else:
            return row[0] + timedelta(days=1)


def retrieve_app_store_downloads(api, sales_report_api_fields, report_date, tmpdir):
    logging.info(f'Quering date: {report_date.strftime("%Y-%m-%d")}')

    report_filters = {"reportDate": report_date.strftime("%Y-%m-%d")}
    report_filters.update(sales_report_api_fields)

    rep_tsv = api.download_sales_and_trends_reports(filters=report_filters)

    with open(tmpdir+'/tmp_{report_date.strftime("%Y-%m-%d")}.txt','w') as outFile:
        outFile.write(rep_tsv)

    df = pd.DataFrame([x.split('\t') for x in rep_tsv.split('\n')])
    new_header = df.iloc[0] #grab the first row for the header
    df = df[1:] #take the data less the header row
    df.columns = new_header #set the header row as the df header

    df = df.dropna()

    df['Begin Date'] = pd.to_datetime(df['Begin Date'])
    df['End Date'] = pd.to_datetime(df['End Date'])

    df.to_csv(tmpdir+f'/tmp_{report_date.strftime("%Y-%m-%d")}.txt',index=False)

    logging.info('App store complete. The data was extracted.')


def main(data, context):

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials_file.json"
    client = bigquery.Client()

    # pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    tmpdir = tempfile.gettempdir()

    api = Api(config.config_vars['KEY_ID'], config.config_vars['PATH_TO_KEY'], config.config_vars['ISSUER_ID'])

    sales_report_api_fields = {"reportType": "SALES", 
                            "frequency": "DAILY", 
                            "reportSubType": "SUMMARY", 
                            "version": "1_0", 
                            "vendorNumber": config.config_vars['VENDOR_NUMBER']}

    json_schema_file = open('schema.json')
    json_schema = json.load(json_schema_file)

    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV,
                                        skip_leading_rows=1, 
                                        autodetect=False,
                                        schema=json_schema,
                                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

    start_date = get_last_sync_date(client)
    end_date = date.today()
    #start_date = date(2022, 9, 1)
    #end_date = date(2022, 9, 5)
    # Daily reports for the Americas are available by 5 am Pacific Time; Japan, Australia, and New Zealand by 5 am Japan Standard Time; and 5 am Central European Time for all other territories. 
    # aka 2pm

    for report_date in daterange(start_date, end_date):
            retrieve_app_store_downloads(api, sales_report_api_fields, report_date, tmpdir)
            

            with open(tmpdir+f'/tmp_{report_date.strftime("%Y-%m-%d")}.txt', "rb") as source_file:
                job = client.load_table_from_file(source_file,config.config_vars['TABLE_ID'],job_config=job_config)

            job.result()

            logging.info('BigQuery complete. The data was uploaded.')

if __name__ == '__main__':
    main('data', 'context')