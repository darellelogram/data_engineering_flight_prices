from google.cloud import bigquery
from google.oauth2 import service_account

credentials_proj = service_account.Credentials.from_service_account_file('bigquery-key.json')
client = bigquery.Client(credentials=credentials_proj)

query_job = client.query(
    """
    SELECT 
    *  
    FROM `is3107-flightprice-23.flight_prices.final` 
    WHERE DATE(_PARTITIONTIME) = "2023-03-19"
    LIMIT 10
    """
)
results = query_job.result()
print(results)

for row in results:
    print("{} flight at ${} on {}".format(row.airline, row.price, row.date))