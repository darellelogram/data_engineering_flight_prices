from google.cloud import storage
import pandas as pd
from io import StringIO
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

credentials_proj = service_account.Credentials.from_service_account_file('bigquery-key.json')
# bq_client = bigquery.Client(credentials=credentials_proj)
bucket_name = 'flight-prices'
storage_client = storage.Client(credentials=credentials_proj)

def readWriteToCloudStorage(input_blob_name, bucket_name, storage_client, date):
    bucket = storage_client.bucket(bucket_name)
    input_blob = bucket.blob(input_blob_name)

    # reda from input
    with input_blob.open("r") as f:
        rawStr = f.read()

    input_blob_name.split('.')[0]

    df = pd.read_csv(StringIO(rawStr))
    df['date'] = df['date'].apply(lambda x: datetime.strptime(x, r'%d-%m-%Y'))
    output_blob_name = 'daily/' + input_blob_name.split('.')[0] + '_' + str(date) + '.csv'
    output_blob = bucket.blob(output_blob_name)

    # maxdate = df[df['date']==df['date'].max()]
    # output_blob.upload_from_string(maxdate.to_csv(), 'text/csv')
    
    filtdate = df[df['date']==date]
    output_blob.upload_from_string(filtdate.to_csv(), 'text/csv')
    # check if uploaded correctly
    with output_blob.open("r") as f:
        rawStr = f.read()
    df_check = pd.read_csv(StringIO(rawStr))
    df_check.head()

date = '2022-03-31'
readWriteToCloudStorage('economy.csv', bucket_name, storage_client, date)