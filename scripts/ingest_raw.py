import pandas as pd
from google.cloud import storage
from google.cloud import bigquery

import os

#Credencial
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\ygorc\OneDrive\Área de Trabalho\aPortifolio\projeto_atual-Radar-Combustiveis\gcp\sa.json"

# Variaveis gcp
NOME_BUCKET = "radar-combustiveis-raw"
NOME_PROJETO = "radar-combustiveis-br"
NOME_DATASET = "raw"
print("variaveis gcp definidas")

# Ler o dataset
df = pd.read_excel("semanal-estados-desde-2013.xlsx",header=17, engine="openpyxl")
print("dataset lido")

#Tratamento
for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].replace("-", None)    



# Fazer upload do arquivo para o gcs (preciso de orientação)
client = storage.Client()
bucket = client.bucket(NOME_BUCKET)
blob = bucket.blob("raw/anp/estados/semanal-estados-desde-2013.xlsx")
blob.upload_from_filename("semanal-estados-desde-2013.xlsx")
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