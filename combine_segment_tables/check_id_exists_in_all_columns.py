from google.cloud import bigquery
import logging
# Construct a BigQuery client object.
client = bigquery.Client()

logging.basicConfig(filename='check_id_exists_in_all_columns.log', encoding='utf-8', level=logging.DEBUG)

mappings = [
    {
        'source':'project_id.android',
        'destination':'project_id.raw_segment_android__staging_events'
    },
    {
        'source':'project_id.android_production',
        'destination':'project_id.raw_segment_android__production_events'
    },
    {
        'source':'project_id.customerio',
        'destination':'project_id.raw_segment_customerio'
    },
    {
        'source':'project_id.revenuecat',
        'destination':'project_id.raw_segment_revenuecat__production'
    },
    {
        'source':'project_id.ios_staging',
        'destination':'project_id.raw_segment_ios__staging_events'
    },    
    {
        'source':'project_id.ios__production',
        'destination':'project_id.raw_segment_ios__production_events'
    },
]
def main():
  
    # Make an API request.

    for mapping in mappings:
        tables = client.list_tables(mapping['source'])
        print("Tables contained in '{}':".format(mapping['source']))
        for table in tables:

            # Check if table exist otherwise copy
            source_table_id = mapping['source']+'.'+table.table_id
            logging.info(f"source_table_id is: {source_table_id}")

            if source_table_id[-4:]=='view':
                continue
            query_column_names = client.query(f"SELECT column_name FROM {mapping['source']}.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table.table_id}'")

            column_list = []
            for c in query_column_names.result():
                column_list.append(c[0])

            if 'id' not in column_list:
                print(f'{source_table_id} does contain id column')

if __name__ == '__main__':
    main()