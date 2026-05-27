import pandas as pd
from google.cloud import storage, bigquery
from config import *

def carregar_function(caminho : str) -> None:
    df = pd.read_parquet(caminho)

    data_ref = df['DATA INICIAL'].max().strftime('%Y-%m-%d')

    # Fazer upload do arquivo para o gcs
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(f"raw/anp/estados/{data_ref}/dados.parquet")
    blob.upload_from_filename(ARQUIVO_TEMP)
    print("arquivo enviado ao GCS")


    #Fazer Upload no BigQuery
    bq  = bigquery.Client()
    cfg = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )
    job = bq.load_table_from_dataframe(
        df,
        TABELA_RAW,
        job_config=cfg
    )
    job.result()  # espera terminar 
    print(f"job finalizado com status: {job.state}")

    ultima_data = df['DATA INICIAL'].max()

    bq.query(f"""
        MERGE `{TABELA_CTRL}` T
        USING (SELECT DATE('{ultima_data}') AS ultima_data_carregada,
                    CURRENT_TIMESTAMP() AS atualizado_em) S
        ON TRUE
        WHEN MATCHED THEN UPDATE SET
            ultima_data_carregada = S.ultima_data_carregada,
            atualizado_em         = S.atualizado_em
        WHEN NOT MATCHED THEN INSERT VALUES
            (S.ultima_data_carregada, S.atualizado_em)
            """).result()

    print(f"controle atualizado: {ultima_data}")
    print(f"Data Minima do lote: {df['DATA INICIAL'].min()}")
    print(f"Data Maxima do Lote: {df['DATA FINAL'].max()}")
    print(f"{len(df)} Linhas novas inseridas")