import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))

from airflow.decorators import dag, task
import pendulum
import pandas as pd
from google.cloud import storage, bigquery
from validar import validar_dataframe

ARQUIVO_XLSX  = "/opt/airflow/scripts/semanal-estados-desde-2013.xlsx"
ARQUIVO_TEMP  = "/tmp/anp_estados.parquet"
BUCKET        = "radar-combustiveis-raw"
PROJETO       = "radar-combustiveis-br"
TABELA_RAW    = "radar-combustiveis-br.raw.precos_estados"

@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2012, 12, 30, tz="America/Fortaleza"),
    catchup=False,
    tags=["anp", "combustiveis"]
)

def pipeline_anp():
    
    @task 
    def extrair() -> str:
        df = pd.read_excel(ARQUIVO_XLSX, header=17, engine="openpyxl")
        
        #Tratamento
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].replace("-", None)   

        df.to_parquet(ARQUIVO_TEMP, index=False)
        print(f"extraido com {df.shape[0]} linhas")
        return ARQUIVO_TEMP
    
    @task
    def validar(caminho: str) -> str:
        df = pd.read_parquet(caminho)
        validar_dataframe(df)
        return caminho
    
    @task
    def carregar(caminho: str) -> None:
        df = pd.read_parquet(caminho)
        NOME_BUCKET = "radar-combustiveis-raw" 

        # Fazer upload do arquivo para o gcs
        client = storage.Client()
        bucket = client.bucket(NOME_BUCKET)
        blob = bucket.blob("raw/anp/estados/semanal-estados-desde-2013.parquet")
        blob.upload_from_filename(ARQUIVO_TEMP)
        print("arquivo enviado ao GCS")


        #Fazer Upload no BigQuery
        bq  = bigquery.Client()
        cfg = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        )
        job = bq.load_table_from_dataframe(
            df,
            "radar-combustiveis-br.raw.precos_estados",
            job_config=cfg
        )
        job.result()  # espera terminar 
        print(f"job finalizado com status: {job.state}")

    carregar(validar(extrair()))

pipeline_anp()