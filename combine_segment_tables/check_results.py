from google.cloud import bigquery
import logging
# Construct a BigQuery client object.
client = bigquery.Client()

logging.basicConfig(filename='check_results.log', encoding='utf-8', level=logging.DEBUG, filemode='w')
# mappings = [
#     {
#         'source':'project_id.android',
#         'destination':'project_id.raw_segment_android__staging_events',
#         'destination_COPY':'project_id.COPY_raw_segment_android__staging_events'
#     },
#     {
#         'source':'project_id.android_production',
#         'destination':'project_id.raw_segment_android__production_events',
#         'destination_COPY':'project_id.COPY_raw_segment_android__production_events'
#     },
#     {
#         'source':'project_id.customerio',
#         'destination':'project_id.raw_segment_customerio',
#         'destination_COPY':'project_id.COPY_raw_segment_customerio'
#     },
#     {
#         'source':'project_id.revenuecat',
#         'destination':'project_id.raw_segment_revenuecat__production',
#         'destination_COPY':'project_id.COPY_raw_segment_revenuecat__production'
#     },
#     {
#         'source':'project_id.ios_staging',
#         'destination':'project_id.raw_segment_ios__staging_events',
#         'destination_COPY':'project_id.COPY_raw_segment_ios__staging_events',
#         'loaded_time': '2022-10-18 11:16:00.000000 UTC'
#     },    
#     {
#         'source':'project_id.ios_production',
#         'destination':'project_id.raw_segment_ios__production_events',
#         'destination_COPY':'project_id.COPY_raw_segment_ios__production_events',
#         'loaded_time': '2022-10-18 11:15:00.000000 UTC'
#     },
# ]

def main():

    mappings = [
    {
        'source':'project_id.ios_production',
        'destination':'project_id.raw_segment_ios__production_events',
        'destination_COPY':'project_id.COPY_raw_segment_ios__production_events',
        'loaded_time': '2022-10-18 11:15:00.000000 UTC'
    },
]

    for mapping in mappings:
        tables = client.list_tables(mapping['source'])
        print("Tables contained in '{}':".format(mapping['source']))

        for table in tables:

            # Check if table exist otherwise copy
            source_table_id = mapping['source']+'.'+table.table_id
            logging.info(f"source_table_id is: {source_table_id}")

            destination_table_id = mapping['destination']+'.'+table.table_id
            logging.info(f"destination_table_id is: {destination_table_id}")
            
            destination_COPY_table_id = mapping['destination_COPY']+'.'+table.table_id
            logging.info(f"destination_COPY_table_id is: {destination_COPY_table_id}")

            if source_table_id[-4:]=='view':
                continue
            
            create_table_if_not_exists_query = f"""
            CREATE TABLE IF NOT EXISTS {destination_COPY_table_id}
            COPY {source_table_id}
            """
            create_table_if_not_exists_job = client.query(create_table_if_not_exists_query)
            logging.info(f"Result of table creation: {create_table_if_not_exists_job.result()}")

            query = f"""
            SELECT *
            FROM {destination_table_id}--173
            where id not in (SELECT id
            FROM {destination_COPY_table_id}
            UNION ALL
            SELECT id
            FROM {source_table_id})
            and loaded_at < '{mapping['loaded_time']}'"""

            job = client.query(query)

            # NUMBER 3 Worked until dataset got too old, so new events had appeared in the destination which weren't in the destination copy
            # query_distinct_ids_before = f"""
            # SELECT count(distinct id) from (
            # SELECT id
            # FROM {destination_COPY_table_id}
            # UNION ALL
            # SELECT id
            # FROM {source_table_id})
            # """

            # job_distinct_ids_before = client.query(query_distinct_ids_before)
            # for r in job_distinct_ids_before.result():
            #     distinct_ids_before_count = r[0]

            # query_distinct_ids_after = f"""
            # SELECT COUNT(distinct id)
            # FROM {destination_table_id}"""

            # job_distinct_ids_after = client.query(query_distinct_ids_after)
            # for r in job_distinct_ids_after.result():
            #     distinct_ids_after_count = r[0]
            # assert(distinct_ids_after_count == distinct_ids_before_count)
            
            # NUMBER 2: Didn't work when the fields where a struct and I couldn't see from data types if they were a struct
            # query = f"""
            # SELECT {columns_str} 
            # FROM {destination_table_id} 
            # EXCEPT DISTINCT
            # SELECT {columns_str}
            # FROM {source_table_id}
            # EXCEPT DISTINCT
            # SELECT {columns_str}
            # FROM {destination_COPY_table_id}
            # """

            # job = client.query(query)
            # for r in job.result():
            #     destination_count = r[0]

            # NUMBER 1: Didn't when both source and destination contained the data
            # source_query = f"""SELECT COUNT(*) FROM {source_table_id}"""
            # source_job = client.query(source_query)

            # destination_query = f"""SELECT COUNT(*) FROM {destination_table_id}"""
            # destination_job = client.query(destination_query)

            # destination_copy_query = f"""SELECT COUNT(*) FROM {destination_COPY_table_id}"""
            # destination_COPY_job = client.query(destination_copy_query)

            # for r in source_job.result():
            #    source_count = r[0]
            
            # for r in destination_job.result():
            #     destination_count = r[0]

            # for r in destination_COPY_job.result():
            #     destination_COPY_count = r[0]
            


            try:
                assert(any(job.result())==False)
            except AssertionError:
                logging.debug(f"destination and source doesn't have the same number of rows for destination: {destination_table_id} and {source_table_id}")
                print(f"destination and source doesn't have the same number of rows for destination: {destination_table_id} and {source_table_id}")

if __name__ == '__main__':
    main()