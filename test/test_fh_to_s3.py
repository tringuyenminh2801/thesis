import pandas as pd

df = pd.read_parquet("test/kin-s3-thesis-1-2024-04-16-20-04-17-22290713-b671-4a96-b2f8-96d192ca622d.parquet")
print(df.count())
