# conversao simples de um arquivo .csv para .parquet

import pandas as pd

df = pd.read_csv('nome_do_arquivo.csv')

df.to_parquet('nome_do_arquivo.parquet', index=False)

