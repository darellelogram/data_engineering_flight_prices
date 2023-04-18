from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from textwrap import dedent
from google.cloud import storage
import pandas as pd
from io import StringIO
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery

with DAG(
    dag_id="StartProjDag",
    default_args={"retries":2},
    description="DAG to kickstart data flow for IS3107 project",
    start_date=pendulum.datetime(2017,1,1,tz="UTC"),
    schedule_interval=None,
    catchup=False,
    tags=["example"],
) as dag:
    dag.doc_md = __doc__

    def clearPrevRunCSVData(**kwargs):
        ti = kwargs["ti"]
        storage_client = storage.Client(credentials=service_account.Credentials.from_service_account_file('scripts/bigquery-key.json'))
        blobs = storage_client.list_blobs('flight-prices', prefix='daily', delimiter='/')
        for blob in blobs:
            print(f'Deleting file {blob.name}')
            blob.delete()

    def clearPrevRunBQData(**kwargs):
        ti = kwargs["ti"]
        for table_name in ['business_raw', 'economy_raw', 'final']:
            bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file('scripts/bigquery-key.json'))
            table_id = "is3107-flightprice-23.flight_prices." + table_name

            destination_table = bq_client.get_table(table_id)  # Make an API request.
            print("deleting from " + table_id)
            print("Starting with {} rows.".format(destination_table.num_rows))
            
            delete_query = "DELETE FROM " + table_id + " WHERE true"

            delete_job = bq_client.query(delete_query)
            delete_job.result()

            # check if all rows deleted
            check_query= "SELECT COUNT(*) FROM " + table_id + " WHERE true"
            check_job = bq_client.query(check_query) 
            check_results = check_job.result()
            for row in check_results:
                print('Ending with ' + str(row[0]) + ' rows.')

    def labelDays(**kwargs):
        ti = kwargs["ti"]
        # read in data (economy.csv, business.csv) from GCP bucket
        bucket_name = 'flight-prices'
        credentials = service_account.Credentials.from_service_account_file('scripts/bigquery-key.json')
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)

        input_blob_names = ['economy.csv', 'business.csv']

        for input_blob_name in input_blob_names:
            input_blob = bucket.blob(input_blob_name)
            with input_blob.open("r") as f:
                rawStr = f.read()
            df = pd.read_csv(StringIO(rawStr))
            df['date'] = df['date'].apply(lambda x: datetime.strptime(x, r'%d-%m-%Y'))
            
            # days between day of scraping and day of flight
            df['days_left'] = (df['date'] - datetime.strptime('2022-02-10', r'%Y-%m-%d')).apply(lambda x: x.days)
            # Day of Week of the flight
            df['dept_day'] = pd.to_datetime(df["date"], format='%Y-%m-%d', errors='coerce').dt.day_name()

            # sort by date in the raw data
            unique_dates = sorted(list(df['date'].unique()))

            # replace earliest date in data by today's date
            # '2022-02-01' => TODAY()
            # '2022-02-02' => TODAY() + days(1)
            # propagate for all days in data
            today = pendulum.today()
            first_day = unique_dates[0]
            day_mapping = {first_day: today.strftime('%Y-%m-%d')}
            for i in range(1, len(unique_dates)):
                original_date = unique_dates[i]
                new_date = today + pd.Timedelta(days=i)
                day_mapping[original_date] = new_date.strftime('%Y-%m-%d')

            df['date'].replace(to_replace = day_mapping, inplace=True)

            # write new CSV file to another blob
            output_blob_name = input_blob_name.split('.')[0] + '_days_labelled' + '.csv'
            output_blob = bucket.blob(output_blob_name)
            output_blob.upload_from_string(df.to_csv(index=False), 'text/csv')                

    clearPrevRunCSVData_task = PythonOperator(
        task_id="clearPrevRunCSVData",
        python_callable=clearPrevRunCSVData,
    )

    clearPrevRunBQData_task = PythonOperator(
        task_id="clearPrevRunBQData",
        python_callable=clearPrevRunBQData,
    )

    labelDays_task = PythonOperator(
        task_id="labelDays",
        python_callable=labelDays,
    )

    (clearPrevRunCSVData_task, clearPrevRunBQData_task) >> labelDays_task