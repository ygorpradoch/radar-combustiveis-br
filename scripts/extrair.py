import pandas as pd
from google.cloud import bigquery
from airflow.exceptions import AirflowSkipException
from config import *

def extrair_function() -> str:
    bq  = bigquery.Client()

    try:
        resultado = bq.query(f"""
            SELECT ultima_data_carregada
            FROM `{TABELA_CTRL}`    
            LIMIT 1
        """).to_dataframe(create_bqstorage_client=False)

        if resultado.empty:
            ultimo_upload = pd.Timestamp("2012-12-29")
            print("Primeira execução: carregando série completa")
        else:
            ultimo_upload = pd.Timestamp(resultado['ultima_data_carregada'].iloc[0])
            print(f"Último upload registrado: {ultimo_upload}")
    except Exception as e:
        print(f"Controle de carga não encontrado: {e}. Usando data de fallback.")
        ultimo_upload = pd.Timestamp("2012-12-29")
    

    df = pd.read_excel(ARQUIVO_XLSX, header=17, engine="openpyxl")
    df = df[df['DATA INICIAL'] > ultimo_upload]
    
    if df.empty:
        raise AirflowSkipException("Nenhum dado novo para processar")

    df.to_parquet(ARQUIVO_TEMP, index=False)
    print(f"extraido com {len(df)} linhas")
    return ARQUIVO_TEMP