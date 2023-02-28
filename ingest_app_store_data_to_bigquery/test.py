"""Function called by PubSub trigger to execute cron job tasks."""
import config
import logging
import tempfile

from datetime import date, timedelta

from appstoreconnect import Api

from main import retrieve_app_store_downloads

'''
gcloud functions deploy --region=europe-west1  main --entry-point main --runtime python39 --trigger-resource app-store-connect --trigger-event google.pubsub.topic.publish --timeout 540s 
'''

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)


def retrieve_app_store_downloads(api, sales_report_api_fields, report_date, tmpdir):
    logging.info(f'Quering date: {report_date.strftime("%Y-%m-%d")}')

    report_filters = {"reportDate": report_date.strftime("%Y-%m-%d")}
    report_filters.update(sales_report_api_fields)

    rep_tsv = api.download_sales_and_trends_reports(filters=report_filters)

    with open(tmpdir+f'/tmp_{report_date.strftime("%Y-%m-%d")}.txt','w') as outFile:
        outFile.write(rep_tsv)

    logging.info('App store complete. The data was extracted.')


def main():

    # pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    tmpdir = 'tmp'

    api = Api(config.config_vars['KEY_ID'], config.config_vars['PATH_TO_KEY'], config.config_vars['ISSUER_ID'])

    sales_report_api_fields = {"reportType": "SALES", 
                            "frequency": "DAILY", 
                            "reportSubType": "SUMMARY", 
                            "version": "1_0", 
                            "vendorNumber": config.config_vars['VENDOR_NUMBER']}

    start_date = date(2022, 8, 30)
    # Daily reports for the Americas are available by 5 am Pacific Time; Japan, Australia, and New Zealand by 5 am Japan Standard Time; and 5 am Central European Time for all other territories. 
    # aka 2pm
    end_date = date(2022, 9, 5)
    # Daily reports for the Americas are available by 5 am Pacific Time; Japan, Australia, and New Zealand by 5 am Japan Standard Time; and 5 am Central European Time for all other territories. 
    # aka 2pm

    for report_date in daterange(start_date, end_date):
            retrieve_app_store_downloads(api, sales_report_api_fields, report_date, tmpdir)
            print(report_date)
            logging.info('BigQuery complete. The data was uploaded.')


            
main()