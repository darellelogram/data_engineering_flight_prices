from google.cloud import bigquery
from google.oauth2 import service_account


def deleteRowsFromBigQueryTable(table_name):
    bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file('scripts/bigquery-key.json'))
    table_id = "is3107-flightprice-23.flight_prices." + table_name

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("deleting from " + table_id)
    print("Starting with {} rows.".format(destination_table.num_rows))
    
    # delete_query = "DELETE FROM " + table_id + " WHERE DATE(_PARTITIONTIME)<=\"2023-03-20\" and true"
    delete_query = "DELETE FROM " + table_id + " WHERE DATE(_PARTITIONTIME)<=CURRENT_DATE() and true"

    delete_job = bq_client.query(delete_query)
    delete_job.result()

    # check if all rows deleted
    # check_query= "SELECT COUNT(*) FROM " + table_id + " WHERE DATE(_PARTITIONTIME)<=\"2023-03-20\""
    check_query= "SELECT COUNT(*) FROM " + table_id + " WHERE DATE(_PARTITIONTIME)<=CURRENT_DATE()"
    check_job = bq_client.query(check_query) 
    check_results = check_job.result()
    for row in check_results:
        print('Ending with ' + str(row[0]) + ' rows.')
# deleteRowsFromBigQueryTable('business_raw')
# deleteRowsFromBigQueryTable("final")