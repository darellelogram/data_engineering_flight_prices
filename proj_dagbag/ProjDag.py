import json
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from textwrap import dedent


with DAG(
    "ProjDag",
    default_args={"retries":2},
    description="DAG for IS3107 project",
    start_date=pendulum.datetime(2023,3,21, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    dag.doc_md = __doc__

    def readFromCSV(**kwargs):

    def writeToDailyCSV(**kwargs):
    
    def 