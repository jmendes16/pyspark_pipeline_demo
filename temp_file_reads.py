import pandas as pd

df = pd.read_parquet('test/temp_member_spend.parquet')

print(df.head())