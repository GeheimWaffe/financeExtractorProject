import pandas as pd
from finance import database as pl

df = pd.DataFrame(data=[[1, 2, 3], [4, 5, 6]], columns=["A", "B", "C"])

pl.save_dataframe_to_sql(df, 'test_table')