from google.cloud import bigquery
import logging
# Construct a BigQuery client object.
client = bigquery.Client()

logging.basicConfig(filename='combine_segment_tables_logs.log', encoding='utf-8', level=logging.DEBUG, filemode='w')
# mappings = [
#     {
#         'source':'project_id.android',
#         'destination':'project_id.raw_segment_android__staging_events'
#     }, - done
#     {
#         'source':'project_id.android_production',
#         'destination':'project_id.raw_segment_android__production_events'
#     }, - done
#     {
#         'source':'project_id.customerio',
#         'destination':'project_id.raw_segment_customerio'
#     }, - done
#     {
#         'source':'project_id.revenuecat',
#         'destination':'project_id.raw_segment_revenuecat__production'
#     }, - done
#     {
#         'source':'project_id.ios_staging',
#         'destination':'project_id.raw_segment_ios__staging_events'
#     },    
#     {
#         'source':'project_id.ios_production',
#         'destination':'project_id.raw_segment_ios__production_events'
#     },
# ]
def main():
  
    mappings = [
     {
         'source':'project_id.ios_production',
         'destination':'project_id.raw_segment_ios__production_events'
     },
    ]
    # Make an API request.

    for mapping in mappings:
        tables = client.list_tables(mapping['source'])
        print("Tables contained in '{}':".format(mapping['source']))
        for table in tables:

            # Check if table exist otherwise copy
            source_table_id = mapping['source']+'.'+table.table_id
            logging.info(f"source_table_id is: {source_table_id}")
            destination_table_id = mapping['destination']+'.'+table.table_id
            logging.info(f"destination_table_id is: {destination_table_id}")
            if source_table_id[-4:]=='view':
                continue
            query_column_names = client.query(f"SELECT column_name FROM {mapping['source']}.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table.table_id}'")

            column_list = []
            for c in query_column_names.result():
                column_list.append(c[0])

            columns_str = ','.join(column_list)

            create_table_if_not_exists_query = f"""
                CREATE TABLE IF NOT EXISTS {destination_table_id}
                COPY {source_table_id}
            """
            create_table_if_not_exists_job = client.query(create_table_if_not_exists_query)
            logging.info(f"Result of table creation: {create_table_if_not_exists_job.result()}")

            query = f"""
            MERGE {destination_table_id} as target
            USING {source_table_id} as source
            on target.id = source.id
            WHEN NOT MATCHED THEN
            INSERT ({columns_str})
            VALUES ({columns_str})"""

            query_job = client.query(query)
            logging.info(f"Result of query results: {query_job.result()}")
        logging.info(f"Done: {mapping}")
    logging.info(f"Done with all mappings.")

if __name__ == '__main__':
    main()