import json
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from textwrap import dedent
from google.cloud import storage
import pandas as pd
from io import StringIO
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.models import Variable

with DAG(
    "ProjDag",
    default_args={"retries":2},
    description="DAG for IS3107 project",
    start_date=pendulum.datetime(2023,3,23, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    dag.doc_md = __doc__
    def initCredentials(**kwargs):
        ti = kwargs["ti"]
        # credentials = service_account.Credentials.from_service_account_file('../scripts/bigquery-key.json')
        # credentials = Variable.set("credentials", service_account.Credentials.from_service_account_file('../scripts/bigquery-key.json'))
        with open('../scripts/bigquery-key.json', 'r') as f:
            credentials_obj = json.load(f)
        Variable.set("credentials", credentials_obj)
        bucket_name = 'flight-prices'
        # ti.xcom_push("credentials", credentials)
        ti.xcom_push("bucket_name", bucket_name)

    # simulates ingestion
    def readBulkWriteDaily(**kwargs):
        ti = kwargs["ti"]
        # credentials = ti.xcom_pull(task_ids="initCredentials", key="credentials")
        # credentials_obj = Variable.get("credentials")
        # with open('../scripts/bigquery-key.json', 'r') as f:
        #     credentials_obj = json.load(f)
        # credentials = service_account.Credentials.from_service_account_info(credentials_obj)
        # credentials = service_account.Credentials.from_service_account_info(Variable.get("credentials"))
        bucket_name = ti.xcom_pull(task_ids="initCredentials", key="bucket_name")
        credentials = service_account.Credentials.from_service_account_file('../scripts/bigquery-key.json')
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        
        input_blob_names = ['economy.csv', 'business.csv']

        for input_blob_name in input_blob_names:
            input_blob = bucket.blob(input_blob_name)
            with input_blob.open("r") as f:
                rawStr = f.read()
            df = pd.read_csv(StringIO(rawStr))
            df['date'] = df['date'].apply(lambda x: datetime.strptime(x, r'%d-%m-%Y'))
            # hard code the date for now
            date='2022-03-30'
            # should be running the dag every day starting from the first day in the data
            # date = pendulum.today() - pd.Timedelta(days=356)
            output_blob_name = 'daily/' + input_blob_name.split('.')[0] + '_' + str(date.replace('-','_')) + '.csv'
            output_blob = bucket.blob(output_blob_name)
            filtdate = df[df['date']==date]
            output_blob.upload_from_string(filtdate.to_csv(index=False), 'text/csv')
            
        # ti.xcom_push()

    def loadToBigQuery(**kwargs):
        print('loading to big query')
        # hard code for now
        input_filenames = ['daily/economy_2022_03_30.csv', 'daily/business_2022_03_30.csv']
        destination_table_names = ['economy_raw', 'business_raw']
        for input_filename, destination_table_name in zip(input_filenames, destination_table_names):
        # input_filename = 'economy.csv'
        # destination_table_name = 'economy_raw'
            uri = "gs://flight-prices/" + input_filename
            table_id = "is3107-flightprice-23.flight_prices." + destination_table_name
            bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file('../scripts/bigquery-key.json'))

            destination_table = bq_client.get_table(table_id)
            print("loading to " + table_id)
            print("Starting with {} rows.".format(destination_table.num_rows))

            job_config = bigquery.LoadJobConfig(
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
                allow_quoted_newlines=True,
                ignore_unknown_values=True
            )

            load_job = bq_client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )
            load_job.result()
            destination_table = bq_client.get_table(table_id)
            print("Ending with {} rows.".format(destination_table.num_rows))

    
    def combineAndClean(**kwargs):
        print('combine and clean in BigQuery')
        # use En Qi's code in airline_clean.py to 
        # combine the business and economy tables into 1
        # and process the columns appropriately
        



    initCredentials_task = PythonOperator(
        task_id="initCredentials",
        python_callable=initCredentials,
    )

    readBulkWriteDaily_task = PythonOperator(
        task_id="readBulkWriteDaily",
        python_callable=readBulkWriteDaily,
    )

    loadToBigQuery_task = PythonOperator(
        task_id="loadToBigQuery",
        python_callable=loadToBigQuery,
    )

    combineAndClean_task = PythonOperator(
        task_id="combineAndClean",
        python_callable=combineAndClean,
    )

    initCredentials_task >> readBulkWriteDaily_task >> loadToBigQuery_task >> combineAndClean_task