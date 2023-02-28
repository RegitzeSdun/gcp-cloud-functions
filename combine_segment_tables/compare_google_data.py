from importlib.util import source_hash
from google.cloud import bigquery
# Construct a BigQuery client object.
client = bigquery.Client()



def main():
  
    source = 'project_id.google_play_data'
    destination = 'project_id.raw_google_play__reports'

    # Make an API request.


    tables = client.list_tables(source)
    print("Tables contained in '{}':".format(source))
    for table in tables:

        source_table_id = source+'.'+table.table_id
        destination_table_id = destination+'.'+table.table_id
        
        if source_table_id[-4:]=='view':
            continue

        destination_query = f"""SELECT COUNT(*) FROM {destination_table_id}"""
        destination_job = client.query(destination_query)

        source_query = f"""SELECT COUNT(*) FROM {source_table_id}"""
        source_job = client.query(source_query)

        for r in source_job.result():
           source_count = r[0]
        
        for r in destination_job.result():
            destination_count = r[0]

        print(f'source_table_id: {source_table_id}, source_count: {source_count}, destination_count: {destination_count}')

if __name__ == '__main__':
    main()