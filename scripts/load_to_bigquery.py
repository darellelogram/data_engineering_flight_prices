# from google.cloud import bigquery_datatransfer
from google.oauth2 import service_account
from google.cloud import bigquery
import ast
# transfer_client = bigquery_datatransfer.DataTransferServiceClient(credentials=credentials_proj)

# TODO: Update to your project ID.
project_id = "is3107-flightprice-23"

credentials_proj = service_account.Credentials.from_service_account_file('bigquery-key.json')
bq_client = bigquery.Client(credentials=credentials_proj)

def loadToBigQuery(input_filename, destination_table_name, bq_client):
    uri = "gs://flight-prices/" + input_filename
    table_id = "is3107-flightprice-23.flight_prices." + destination_table_name

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("loading to " + table_id)
    print("Starting with {} rows.".format(destination_table.num_rows))

    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        allow_quoted_newlines=True,
        ignore_unknown_values=True
    )

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Ending with {} rows.".format(destination_table.num_rows))

loadToBigQuery("business.csv", "business_raw", bq_client)
loadToBigQuery("economy.csv", "economy_raw", bq_client)


