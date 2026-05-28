import pandera as pa
from pandera import Check, Column
import pandas as pd

estados_validos = ['ACRE' ,'ALAGOAS', 'AMAPA', 'AMAZONAS', 'BAHIA', 'CEARA', 'DISTRITO FEDERAL',
 'ESPIRITO SANTO', 'GOIAS', 'MARANHAO', 'MATO GROSSO', 'MATO GROSSO DO SUL',
 'MINAS GERAIS', 'PARA', 'PARAIBA', 'PARANA', 'PERNAMBUCO', 'PIAUI',
 'RIO DE JANEIRO', 'RIO GRANDE DO NORTE', 'RIO GRANDE DO SUL', 'RONDONIA',
 'RORAIMA', 'SANTA CATARINA', 'SAO PAULO', 'SERGIPE', 'TOCANTINS']

produtos_validos = ['ETANOL HIDRATADO', 'GASOLINA COMUM', 'OLEO DIESEL', 'OLEO DIESEL S10', 'GASOLINA ADITIVADA']

unidades_medida_validas = ['R$/l']

schema = pa.DataFrameSchema({
    "UNIDADE DE MEDIDA" : Column(str, Check.isin(unidades_medida_validas)),
    "PRODUTO" : Column(str, Check.isin(produtos_validos)),
    "ESTADO" : Column(str, Check.isin(estados_validos)),
    "DATA INICIAL": Column("datetime64[ns]"),
    "PREÇO MÉDIO REVENDA": Column(float, Check.gt(0))
})



def validar_dataframe(caminho):
    df = pd.read_parquet(caminho)
    schema.validate(df, lazy=True)
    print(f"Validação ok — {len(df):,} linhas verificadas")
    return caminho


