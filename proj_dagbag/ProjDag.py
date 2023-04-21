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

#TODO: modularize code further - put initialisation of credentials, 
# Storage Clients and BigQuery clients into separate functions
# possibly pass them into airflow Variables using Variable.set()
# or make use of ti.xcom_push (alr tried passing the original json file containing the key, 
# but "default credentials" not found on the Google side)
# so far the only thing that works is directly initialising the service_account.Credentials
# in the function itself where it is going to be used to initialise the client

#TODO: make dates dynamic, write airflow schedule

#TODO: take makeDurationCategory and makeTimeBin functions out of 
# the combineAndClean function?

#TODO: explore using airflow Variables to cache dataframes or at least 2D lists
# or perhaps even caching in local storage

with DAG(
    dag_id="ProjDag",
    default_args={"retries":2},
    description="DAG for IS3107 project",
    start_date=pendulum.datetime(2017,1,1,tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"],
) as dag:
    dag.doc_md = __doc__

    # simulates ingestion
    def readBulkWriteDaily(**kwargs):
        ti = kwargs["ti"]
        bucket_name = 'flight-prices'
        credentials = service_account.Credentials.from_service_account_file('scripts/bigquery-key.json')
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        
        input_blob_names = ['economy_days_labelled.csv', 'business_days_labelled.csv']

        for input_blob_name in input_blob_names:
            input_blob = bucket.blob(input_blob_name)
            with input_blob.open("r") as f:
                rawStr = f.read()
            df = pd.read_csv(StringIO(rawStr))
            
            date = pendulum.today().strftime('%Y-%m-%d')
            
            # daily files will already have 
            output_blob_name = 'daily/' + input_blob_name.split('.')[0] + '_' + str(date) + '.csv'
            output_blob = bucket.blob(output_blob_name)
            filtdate = df[df['date']==date]
            output_blob.upload_from_string(filtdate.to_csv(index=False), 'text/csv')
            

    def loadRawToBigQuery(**kwargs):
        print('loading to big query')
        bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file('scripts/bigquery-key.json'))

        input_blob_names = ['economy_days_labelled.csv', 'business_days_labelled.csv']
        date = pendulum.today().strftime('%Y-%m-%d')
        input_filenames = []
        for input_blob_name in input_blob_names:
            input_filename = 'daily/' + input_blob_name.split('.')[0] + '_' + str(date) + '.csv'
            input_filenames.append(input_filename)
        destination_table_names = ['economy_raw', 'business_raw']
        for input_filename, destination_table_name in zip(input_filenames, destination_table_names):
            uri = "gs://flight-prices/" + input_filename
            table_id = "is3107-flightprice-23.flight_prices." + destination_table_name

            destination_table = bq_client.get_table(table_id)
            print("loading to " + table_id)
            print("Starting with {} rows.".format(destination_table.num_rows))

            job_config = bigquery.LoadJobConfig(
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
                allow_quoted_newlines=True,
                ignore_unknown_values=True,
                write_disposition="WRITE_TRUNCATE",
            )

            load_job = bq_client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )
            load_job.result()
            destination_table = bq_client.get_table(table_id)
            print("Ending with {} rows.".format(destination_table.num_rows))

    def combineAndClean(**kwargs):
        print('combine and clean in BigQuery')
        # combine the business and economy tables into 1
        # and process the columns appropriately
        ti = kwargs["ti"]

        # pull from BigQuery
        bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file('scripts/bigquery-key.json'))

        econ_query = """SELECT * FROM is3107-flightprice-23.flight_prices.economy_raw"""
        econ = bq_client.query(econ_query).to_dataframe()

        biz_query = """SELECT * FROM is3107-flightprice-23.flight_prices.business_raw"""
        biz = bq_client.query(biz_query).to_dataframe()
        print('biz.shape is ' + str(biz.shape))
        print('biz.columns is ' + str(biz.columns))
        print('biz is')
        print(biz)
        biz["class"] = "business"
        econ["class"] = "economic"

        coord_query = """SELECT * FROM is3107-flightprice-23.flight_prices.india_cities"""
        coord = bq_client.query(coord_query).to_dataframe()

        combi = pd.concat([econ, biz])

        combi.rename({"dep_time": "departure_time", "from": "source_city", 
                    "time_taken": "duration", "stop": "stops", "arr_time": "arrival_time",
                "to":"destination_city"}, axis = 1, inplace = True)
        combi['duration'] = combi['duration'].astype(str)


        dd = pd.DataFrame(combi["date"].astype(str).str.split("-",expand = True).to_numpy().astype(int),columns = ["year","month","day"])

        temp = pd.DataFrame(combi["duration"].str.split(expand = True).to_numpy().astype(str), 
                            columns = ["hour","minute"])

        temp["hour"] = temp["hour"].apply(lambda x: re.sub("[^0-9]","",x)).astype(int)
        temp["minute"] = temp["minute"].apply(lambda r: re.sub("[^0-9]","",r))  
        temp["minute"] = np.where(temp["minute"] == "", 0, temp["minute"]) 
        temp["minute"] = temp["minute"].astype(int) #converting data type

        combi["duration"] = np.around((temp["hour"] + (temp["minute"]/60)),2)

        combi["stops"] = combi["stops"].apply(lambda r: re.sub("[^0-9]","",r)) # taking only digits
        combi["stops"] = np.where(combi["stops"] == "", 0, combi["stops"]) # replacign "" with 0
        combi["stops"] = combi["stops"].astype(int)


        def makeDurationCategory(x):
            if x < 3:
                return 'Short Haul'
            elif x < 6:
                return 'Medium Haul'
            else:
                return 'Long Haul'
            
        combi['duration_category'] = combi['duration'].astype(int).apply(makeDurationCategory)

        def makeTimeBin(x):
            if (x >= '00:00:00') & (x < '04:00:00'):
                return 'Midnight'
            elif (x >= "04:00:00") & (x < "08:00:00"):
                return "Early Morning"
            elif (x >= "08:00:00") & (x <"12:00:00"):
                return "Late Morning"
            elif (x >= "12:00:00") & (x < "16:00:00") :
                return "Afternoon"
            elif (x >= "16:00:00") & (x < "20:00:00"):
                return "Evening"
            else:
                return "Night"
            
        combi["departure_time"] = pd.to_datetime(combi["departure_time"], format="%H:%M")
        combi["departure_time"] = combi["departure_time"].dt.strftime("%H:%M:%S")
        combi["departure_time_bin"] = combi["departure_time"].apply(makeTimeBin)

        combi["arrival_time"] = pd.to_datetime(combi["arrival_time"], format="%H:%M")
        combi["arrival_time"] = combi["arrival_time"].dt.strftime("%H:%M:%S")
        combi["arrival_time_bin"] = combi["arrival_time"].apply(makeTimeBin)

        combi = pd.merge(combi,coord, left_on="source_city", right_on="city", how="left")
        combi.rename({"latitude": "source_latitude", "longitude": "source_longitude"}, axis = 1, inplace = True)

        combi = pd.merge(combi, coord, left_on="destination_city", right_on="city", how="left")
        combi.rename({"latitude": "destination_latitude", "longitude": "destination_longitude"}, axis = 1, inplace = True)

        combi.drop(columns=["city_x", "country_x", "city_y","country_y"], inplace=True)

        table_id = "is3107-flightprice-23.flight_prices.final"
        
        table = bq_client.get_table(table_id)
        print("Loading to {}".format(table_id))
        print("Starting with {} rows".format(table.num_rows))

        load_clean_job = bq_client.load_table_from_dataframe(
            combi, table_id
        )
        load_clean_job.result()

        table = bq_client.get_table(table_id)
        print("Ending with {} rows".format(table.num_rows))

    def buildMlModel(**kwargs):
        print("building ML Model")
        bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file('scripts/bigquery-key.json'), 
                                    project='is3107-flightprice-23', location='asia-southeast1')
        # Train a RandomForestRegressor
        train_query = """
        CREATE OR REPLACE MODEL is3107-flightprice-23.ml_model.rfr_model
        OPTIONS(MODEL_TYPE='RANDOM_FOREST_REGRESSOR',
                MAX_TREE_DEPTH = 20,
                INPUT_LABEL_COLS = ['price']
                )
        AS SELECT 
        airline,source_city,destination_city,price,class,days_left,duration_category,departure_time_bin,dept_day
        FROM `is3107-flightprice-23.flight_prices.final`;
        """
        bq_client.query(train_query)
        print("ML Model Built")

        # Model Performance
        print("Model Performance:")
        eval_query = """
        SELECT *
        FROM ML.EVALUATE(MODEL `is3107-flightprice-23.ml_model.rfr_model`)
        """
        evaluation = bq_client.query(eval_query).to_dataframe()
        print(evaluation)
        print("End of Model")

    readBulkWriteDaily_task = PythonOperator(
        task_id="readBulkWriteDaily",
        python_callable=readBulkWriteDaily,
    )

    loadRawToBigQuery_task = PythonOperator(
        task_id="loadRawToBigQuery",
        python_callable=loadRawToBigQuery,
    )

    combineAndClean_task = PythonOperator(
        task_id="combineAndClean",
        python_callable=combineAndClean,
    )

    buildMlModel_task = PythonOperator(
        task_id="buildMlModel",
        python_callable=buildMlModel,
    )

    # readBulkWriteDaily_task >> loadRawToBigQuery_task >> combineAndClean_task
    readBulkWriteDaily_task >> loadRawToBigQuery_task >> combineAndClean_task >> buildMlModel_task