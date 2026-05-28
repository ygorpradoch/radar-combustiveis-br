import pandas as pd
from config import *

PRODUTOS_POSTO = [
    "GASOLINA COMUM",
    "GASOLINA ADITIVADA",
    "ETANOL HIDRATADO",
    "OLEO DIESEL",
    "OLEO DIESEL S10",
]

def tratar_function(caminho: str) -> str:
    df = pd.read_parquet(caminho)

    # Substitui traços por None (nulos disfarçados da ANP)
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].replace("-", None)

    # Filtra apenas produtos vendidos em postos de combustível
    df = df[df["PRODUTO"].isin(PRODUTOS_POSTO)]

    df.to_parquet(caminho, index=False)
    print(f"Tratado: {len(df)} linhas | Produtos: {df['PRODUTO'].unique().tolist()}")
    return caminho