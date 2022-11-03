### Importando bibliotecas

import pandas as pd
import numpy as np
from binance.client import Client
import psycopg2
import os
from airflow import DAG
from datetime import datetime 
from airflow.operators.python import PythonOperator


## Iniciando a DAG

with DAG("ETL_Binance", start_date=datetime(2022, 11, 1),schedule_interval="30 * * * *", catchup=False) as dag: