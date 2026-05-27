import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))

from airflow.decorators import dag, task
import pendulum

from validar import validar_dataframe
from extrair import extrair_function
from carregar import carregar_function
from atualizar_stg import atualizar_stg_function
from atualizar_mart import atualizar_mart_function


@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2012, 12, 30, tz="America/Fortaleza"),
    catchup=False,
    tags=["anp", "combustiveis"]
)

def pipeline_anp():
    
    
    @task 
    def extrair() -> str:
        return extrair_function()
    
    @task
    def validar(caminho: str) -> str:
        return validar_dataframe(caminho)
    
    @task
    def carregar(caminho: str) -> None:
        return carregar_function(caminho)

    @task
    def atualizar_staging() -> None:
        return atualizar_stg_function()

    @task
    def atualizar_mart() -> None:
        return atualizar_mart_function()
        


    carregado = carregar(validar(extrair()))
    carregado >> atualizar_staging() >> atualizar_mart()

pipeline_anp()