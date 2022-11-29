import pandas as pd

df = pd.read_parquet("./datasets/364_v4_train_batch_0.parquet")

print(df.head())