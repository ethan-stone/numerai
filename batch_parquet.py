import pandas as pd
import pyarrow.parquet as pq 
from sqlalchemy import create_engine


conn_string = "postgresql://postgres:password@localhost/postgres"

parquet_file = pq.ParquetFile("./datasets/364_v4_train.parquet")
db = create_engine(conn_string)
conn = db.connect()


for batch in parquet_file.iter_batches(batch_size=1000):
    df: pd.DataFrame = batch.to_pandas()
    df.to_sql("numerai_round_364_v4", con=conn, if_exists="append", index=False)

conn.close()
db.dispose()