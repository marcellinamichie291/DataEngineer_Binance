import pandas as pd
import numpy as np
from binance.client import Client
import psycopg2
import os

def extract_bitcoin():

    global df
    
    api_key = os.getenv(key='BINANCE_API_KEY')
    api_secret = os.getenv(key='BINANCE_SECRET_KEY')

    client = Client(api_key, api_secret)

    # Consultando a API da Binance

    ar = np.array(
    client.get_historical_klines(
        symbol='BTCUSDT',
        interval=Client.KLINE_INTERVAL_1DAY,
        start_str='1 Jan, 2020'
        )
    )
    
    # Transformando dados em um DataFrame

    df = pd.DataFrame(
    ar, 
    dtype=float, 
    columns=(
        'Open_Time',
        'Open_Price',
        'High_Price',
        'Low_Price',
        'Close_Price',
        'Volume',
        'Close_Time',
        'Quote_Asset_Volume',
        'Number_of_Trades',
        'Taker buy base asset volume',
        'Taker buy quote asset volume',
        'Unused field'
    ))
    
    # Convertendo as colunas para datetime

    df['Open_Time'] = pd.to_datetime(df['Open_Time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
    df['Close_Time'] = pd.to_datetime(df['Close_Time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
    
    # Selecionando colunas desejadas
    
    df = df[[
        'Open_Time',
        'Open_Price',
        'High_Price',
        'Low_Price',
        'Close_Price',
        'Volume',
        'Close_Time',
        'Number_of_Trades'
    ]]

    return df

## Adicionando variáveis ao dataframe

def Bollinger():

    global df

    # Média móvel
    df['Media_Movel'] = df['Close_Price'].shift(1).rolling(20).mean()

    # Desvio padrão
    df['Desvio_Padrao'] = df['Close_Price'].shift(1).rolling(20).std()

    # Banda superior
    df['Banda_Superior'] = df['Media_Movel'] + 2*df['Desvio_Padrao']

    # Banda inferior
    df['Banda_Inferior'] = df['Media_Movel'] - 2*df['Desvio_Padrao']

    return df

# Carregando dados na base

def load_data():

    global df

    # Fazendo a conexão

    try:
        conn = psycopg2.connect(database = "postgres", user = "postgres", password = "feararo", host = "localhost", port = "5432")
    except:
        print("I am unable to connect to the database") 

    cur = conn.cursor()

    # Excluindo a tabela

    try:
        cur.execute("DROP TABLE IF EXISTS bitcoin")
    except:
        print("I can't drop our database!")
    
    conn.commit()

    # Criando a tabela
    try:
        cur.execute("""
                        CREATE TABLE bitcoin (
                        Open_Time TIMESTAMP,
                        Open_Price FLOAT,
                        High_Price FLOAT,
                        Low_Price FLOAT,
                        Close_Price FLOAT,
                        Volume INT,
                        Close_Time TIMESTAMP,
                        Number_of_Trades INT,
                        Media_Movel VARCHAR,
                        Desvio_Padrao VARCHAR,
                        Banda_Superior VARCHAR,
                        Banda_Inferior VARCHAR);
                    """)
    except:
        print("I can't create our database!")
    
    conn.commit()

    # Inserindo os dados

    queries = list()

    for i in df.index:
        query = (f"""
                    INSERT INTO bitcoin (Open_Time,
                                        Open_Price, 
                                        High_Price, 
                                        Low_Price, 
                                        Close_Price, 
                                        Volume, 
                                        Close_Time, 
                                        Number_of_Trades, 
                                        Media_Movel, 
                                        Desvio_Padrao, 
                                        Banda_Superior, 
                                        Banda_Inferior)
                    VALUES ('{btc['Open_Time'][i]}',
                            {btc['Open_Price'][i]},
                            {btc['High_Price'][i]},
                            {btc['Low_Price'][i]},
                            {btc['Close_Price'][i]},
                            {btc['Volume'][i]},
                            '{btc['Close_Time'][i]}',
                            {btc['Number_of_Trades'][i]},
                            '{btc['Media_Movel'][i]}',
                            '{btc['Desvio_Padrao'][i]}',
                            '{btc['Banda_Superior'][i]}',
                            '{btc['Banda_Inferior'][i]}')""")
        queries.append(' '.join(query.split()))

    try:
        for query in queries:
            cur.execute(query)
    except:
        print("I am unable to insert the data!")

    conn.commit()
    
    conn.close()
    cur.close()
