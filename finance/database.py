# This module is a generic module for loading and exporting from the finance database
# from pandas in Postgresql
import sqlalchemy as sqla
from pandas import DataFrame

CONNECTION_STRING = 'postgresql://regular_user:user_password@localhost:5432/finance'

def load_to_table(table_name: str, content: DataFrame) -> int:
    """
     Save the data frame to the PostGres database
     :param df: the data frame to save
     :param tablename: the target table
     :return:
     """
    # create the engine
    e = sqla.create_engine(CONNECTION_STRING)
    r: sqla.CursorResult
    # truncate the table. Necessary as there are views built on the schema.
    with e.connect() as conn:
        # truncate the table
        conn.execute(sqla.text('TRUNCATE ' + table_name))

    # append the data
    # Append the new data
    result = int(content.to_sql(table_name, e, schema='public', if_exists='append'))
    return result