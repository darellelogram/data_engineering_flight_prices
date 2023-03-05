import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from textwrap import dedent
# from __future__ import annotations

API_KEY = '3YH8NB17IJPNGDP34ZR7BW3N3PPJF46TB3'
API_1_URL= f'https://api.etherscan.io/api?module=proxy&action=eth_blockNumber&apikey={API_KEY}'
API_2_URL = f'https://api.etherscan.io/api?module=proxy&action=eth_getBlockByNumber&tag=0xff8a53&boolean=true&apikey={API_KEY}'

with DAG(
    "tryOutProjDag",
    default_args={"retries":2},
    description="DAG tutorial",
    schedule=None,
    start_date=pendulum.datetime(2023,5,3, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    dag.doc_md = __doc__

    def get_block_num(**kwargs):
        ti = kwargs["ti"]
        res = requests.get(API_1_URL)
        res = res.json()
        block_num = res['result']
        print('block number: ' + str(block_num))
        ti.xcom_push("block_num", block_num)

    def get_transaction_num(**kwargs):
        ti = kwargs["ti"]
        block_num_str = ti.xcom_pull(task_ids="get_block_num", key="block_num")
        res = requests.get(API_2_URL)
        res = res.json()
        numTransactions = len(res['result']['transactions'])
        print('txs number: ' + str(numTransactions))
    
    get_block_num_task = PythonOperator(
        task_id="get_block_num",
        python_callable=get_block_num,
    )
    get_block_num_task.doc_md = dedent(
        """
        #### Get Block Number tas
        A simple task to retrieve the latest Ethereum block number.
        This block number is printed then pushed to xcom so that it can be processed by the next task.
        """
    )

    get_transaction_num_task = PythonOperator(
        task_id="get_transaction_num",
        python_callable=get_transaction_num,
    )
    get_transaction_num_task.doc_md = dedent(
        """
        #### Get Transaction Number Task
        A simple task to get the number of transactions in the given block number.
        This number if then printed.
        """
    )

    get_block_num_task >> get_transaction_num_task