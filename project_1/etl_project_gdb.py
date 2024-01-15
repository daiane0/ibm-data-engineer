# Code for ETL operations on Country-GDB data

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime


def extract(url, table_attributes):

    '''extrai informações necessárias do site e salva em um dataframe.retorna o dataframe'''

    pag = requests.get(url).text
    data = BeautifulSoup(pag, 'html.parser')
    df = pd.DataFrame(columns=table_attributes)
 # reunir todos os atributos de um tipo específico
    tabelas = data.find_all('tbody')
    linhas = tabelas[2].find_all('tr')
    for linha in linhas:
        col = linha.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"pais": col[0].a.contents[0],
                             "pib_usd_milhoes": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)

    return df


def transform(df):

    '''transforma os dados de formato de moeda para flutuante, de milhoes para bilhoes arredondando para duas casas decimais'''
    pib_list = df["pib_usd_milhoes"].tolist()
    pib_list = [float("".join(x.split(','))) for x in pib_list]
    pib_list = [np.round(x/1000,2) for x in pib_list]
    df["pib_usd_milhoes"] = pib_list
    df=df.rename(columns = {"pib_usd_milhoes":"pib_usd_bilhoes"})
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    '''salva o dataframe final como uma tabela de banco de dados com o nome fornecido'''


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    '''execulta a consulta declarada na tabela do banco de dados e imprime a saída no terminal'''


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
    '''registra a mensagem mencionada em um determinado estágio da execução em um arquivo de log'''


url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

table_attribs = ["pais", "pib_usd_milhoes"]

db_name = 'Economias_mundo.db'

table_name = 'PIB_paises'

csv_path = './PIB_paises.csv'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE pib_usd_bilhoes >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()

