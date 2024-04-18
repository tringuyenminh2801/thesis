import pandas as pd

df = pd.read_parquet(r'test\fh-20240420-2-2024-04-18-15-03-43-6c97dd2a-4b4e-36e5-a3a6-bbdba99cc61c.parquet')
print(df.count())
