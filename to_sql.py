import pandas as pd
from sqlalchemy import create_engine

conn_string = "postgresql://postgres:password@localhost/postgres"

db = create_engine(conn_string)
conn = db.connect()

df = pd.read_parquet("./datasets/364_v4_train_batch_0.parquet")

df.to_sql("numerai_round_364_v4", con=conn, if_exists="append", index=False)

print("inserted")
conn.close()
db.dispose()