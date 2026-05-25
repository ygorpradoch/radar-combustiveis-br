import pandas as pd
import sys

df = pd.read_excel("semanal-estados-desde-2013.xlsx", header=17, engine="openpyxl")



print(df.types())
