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

        ## try using Variables to cache json key
        # credentials = service_account.Credentials.from_service_account_file('scripts/bigquery-key.json')
        # credentials = Variable.set("credentials", service_account.Credentials.from_service_account_file('scripts/bigquery-key.json'))
        with open('scripts/bigquery-key.json', 'r') as f:
            credentials_obj = json.load(f)
        Variable.set("credentials", credentials_obj)
        bucket_name = 'flight-prices'

        ## try using ti.xcom_push to pass credentials object to next fn
        # ti.xcom_push("credentials", credentials)
        ti.xcom_push("bucket_name", bucket_name)

    # simulates ingestion
    def readBulkWriteDaily(**kwargs):
        ti = kwargs["ti"]
        ## try using ti.xcom_pull to receive credentials object from prev fn
        # credentials = ti.xcom_pull(task_ids="initCredentials", key="credentials")
        
        ## try getting credentials json from Variable
        # credentials_obj = Variable.get("credentials")
        # with open('scripts/bigquery-key.json', 'r') as f:
        #     credentials_obj = json.load(f)
        # credentials = service_account.Credentials.from_service_account_info(credentials_obj)
        # credentials = service_account.Credentials.from_service_account_info(Variable.get("credentials"))
        
        # bucket_name = ti.xcom_pull(task_ids="initCredentials", key="bucket_name")
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
            # df['date'] = df['date'].apply(lambda x: datetime.strptime(x, r'%d-%m-%Y'))
            
            # hard code the date for now
            # date='2022-03-29'
            date = pendulum.today().strftime('%Y-%m-%d')
            # should be running the dag every day starting from the first day in the data
            # date = pendulum.today() - pd.Timedelta(days=<numberOfDaysSinceFirstDayInData>)
            
            # daily files will already have 
            output_blob_name = 'daily/' + input_blob_name.split('.')[0] + '_' + str(date) + '.csv'
            output_blob = bucket.blob(output_blob_name)
            filtdate = df[df['date']==date]
            output_blob.upload_from_string(filtdate.to_csv(index=False), 'text/csv')
            

    def loadRawToBigQuery(**kwargs):
        print('loading to big query')
        bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file('scripts/bigquery-key.json'))
        Variable.set("bq_client", bq_client)

        # hard filenames code for now
        # 2022-02-01 to 2022-03-31
        # 2023-04-06 -> 2022-02-01
        # 2023-04-07 -> 2022-02-02
        input_blob_names = ['economy_days_labelled.csv', 'business_days_labelled.csv']
        # date = pendulum.today()
        # print("today is " + str(date))
        # date = '2022-03-30'
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
        ti = kwargs["ti"]

        # pull from BigQuery
        # bq_client = Variable.get("bq_client")
        bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file('scripts/bigquery-key.json'))
        # date = '2022-03-30'

        econ_query = """SELECT * FROM is3107-flightprice-23.flight_prices.economy_raw"""
        econ = bq_client.query(econ_query).to_dataframe()

        # DEBUGGING
        print('econ.shape is ' + str(econ.shape))
        print('econ.columns is ' + str(econ.columns))
        print('econ is')
        print(econ)

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

        # DEBUGGING
        print('combi.shape is ' + str(combi.shape))
        print('combi.columns is ' + str(combi.columns))
        combi["dept_day"] = pd.to_datetime(combi["date"], format='%Y-%m-%d', errors='coerce').dt.day_name()
        combi["num_code"] = combi["num_code"].astype(int)

        combi.rename({"dep_time": "departure_time", "from": "source_city", 
                    "time_taken": "duration", "stop": "stops", "arr_time": "arrival_time",
                "to":"destination_city"}, axis = 1, inplace = True)
        combi['duration'] = combi['duration'].astype(str)


        dd = pd.DataFrame(combi["date"].astype(str).str.split("-",expand = True).to_numpy().astype(int),columns = ["year","month","day"])
        combi["days_left"] = np.where(dd["month"] > 2, dd["day"] +18, np.where(dd["month"] == 2, dd["day"] -10, dd["day"]))

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

        combi.drop(columns=["city_x", "country_x","city_y","country_y"], inplace=True)

        # DEBUGGING
        print('after transformation, combi.columns is ' + str(combi.columns))
        print('combi.shape is ' + str(combi.shape))
        print('combi.shape')
        print([str(col) + str(type(x)) for x, col in zip(list(combi.iloc[0]), combi.columns)])


        table_id = "is3107-flightprice-23.flight_prices.final"
        
        table = bq_client.get_table(table_id)
        print("Loading to {}".format(table_id))
        print("Starting with {} rows".format(table.num_rows))

        load_clean_job_config = bigquery.LoadJobConfig(schema=[
            bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("airline", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("ch_code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("num_code", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("departure_time", bigquery.enums.SqlTypeNames.TIME),
            bigquery.SchemaField("source_city", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("duration", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("stops", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("arrival_time", bigquery.enums.SqlTypeNames.TIME),
            bigquery.SchemaField("destination_city", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("price", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("class", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("dept_day", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("days_left", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("duration_category", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("departure_time_bin", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("arrival_time_bin", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("source_latitude", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("source_longitude", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("destination_latitude", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("destination_longitude", bigquery.enums.SqlTypeNames.FLOAT)
            ]
        )

        load_clean_job = bq_client.load_table_from_dataframe(
            combi, table_id
        )
        load_clean_job.result()

        table = bq_client.get_table(table_id)
        print("Ending with {} rows".format(table.num_rows))

    initCredentials_task = PythonOperator(
        task_id="initCredentials",
        python_callable=initCredentials,
    )

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

    initCredentials_task >> readBulkWriteDaily_task >> loadRawToBigQuery_task >> combineAndClean_task