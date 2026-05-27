from config import *
from google.cloud import bigquery

def atualizar_mart_function():        
    bq  = bigquery.Client()
            
    bq.query(f"""CREATE OR REPLACE TABLE `{TABELA_FATO}`
                PARTITION BY data_inicial
                CLUSTER BY estado, produto
                AS
                SELECT * FROM `{TABELA_STG}`""").result()
    print("mart.fato_preco_estados Atualizado!")