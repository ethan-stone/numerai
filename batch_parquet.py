import pandas as pd
import pyarrow.parquet as pq 

parquet_file = pq.ParquetFile("./datasets/364_v4_train.parquet")

batch_num = 0
for batch in parquet_file.iter_batches():
    df: pd.DataFrame = batch.to_pandas()
    df.to_parquet(f"./datasets/364_v4_train_batch_{batch_num}.parquet")
    batch_num += 1

