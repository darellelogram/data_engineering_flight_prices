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
import numpy as np
import re
import db_dtypes

with DAG(
    "StartProjDag",
    default_args={"retries":2},
    description="DAG to kickstart data flow for IS3107 project",
    start_date=pendulum.datetime(2023,4,16, tz="UTC"),
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    
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
    
    labelDays_task = PythonOperator(
        task_id="labelDays",
        python_callable=labelDays,
    )
