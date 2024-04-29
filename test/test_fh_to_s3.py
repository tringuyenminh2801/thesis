import pandas as pd

df = pd.read_csv("data/frt_data.csv")
print(df['Revenue'].describe())
print(df.dtypes)
