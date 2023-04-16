# from google.cloud import bigquery_datatransfer
from google.oauth2 import service_account
from google.cloud import bigquery
import ast
# transfer_client = bigquery_datatransfer.DataTransferServiceClient(credentials=credentials_proj)

# TODO: Update to your project ID.
project_id = "is3107-flightprice-23"

credentials_proj = service_account.Credentials.from_service_account_file('bigquery-key.json')
bq_client = bigquery.Client(credentials=credentials_proj)


def loadToBigQuerySQL(input_filename, destination_table_name, bq_client):
    uri = "gs://flight-prices/" + input_filename
    table_id = "is3107-flightprice-23.flight_prices." + destination_table_name
    query_job = bq_client.query(
        """
        SELECT COUNT(*) FROM
        `is3107-flightprice-23.flight_prices.business_raw`
        WHERE DATE(_PARTITIONTIME)="2023-03-20"
        """
    )
    query_results = query_job.result()
    for row in query_results:
        print('loading to ' + table_id)
        print('Starting with ' + str(row[0]) + ' rows.')

    # load data into dataset in BigQuery
    load_query = """LOAD DATA 
    INTO""" + 
    table_id +
    """
    FROM FILES(
        format='CSV',
        uris=['gs://flight-prices/""" + input_filename + """'],
        skip_leading_rows=1,
        allow_quoted_newlines=TRUE,
        ignore_unknown_values=TRUE,
        encoding="UTF-8"
        )
    """
    load_job = bq_client.query(load_query)

    load_results = load_job.result()

    # check if load was successful
    check_job = bq_client.query(
        """
        SELECT COUNT(*) FROM
        r"{table_id}"
        WHERE DATE(_PARTITIONTIME)="2023-03-20"
        """
    )
    check_results = check_job.result()
    # print(results)
    for row in check_results:
        print('Ending with ' + str(row[0]) + ' rows.')

loadToBigQuerySQL("business.csv", "business_raw", bq_client)
loadToBigQuerySQL("economy.csv", "economy_raw", bq_client)

