# This module is a generic module for loading and exporting from the finance database
# from pandas in Postgresql
import sqlalchemy as sqla
import sqlalchemy.exc
from pandas import DataFrame
from sqlalchemy.orm import Session

CONNECTION_STRING = 'postgresql://regular_user:userpassword@localhost:5432/finance'

def load_to_table(table_name: str, content: DataFrame) -> int:
    """
     Save the data frame to the PostGres database
     :param df: the data frame to save
     :param tablename: the target table
     :return:
     """
    # create the engine
    e = sqla.create_engine(CONNECTION_STRING)
    with Session(e) as session:
        try:
            r = session.execute(sqla.text('TRUNCATE ' + table_name))
            session.commit()
        except sqla.exc.ProgrammingError:
            print('no table found')

    # append the data
    # Append the new data
    result = int(content.to_sql(table_name, e, schema='public', if_exists='append'))
    return result

def get_row_count(table_name: str) -> int:
    # create the engine
    e = sqla.create_engine(CONNECTION_STRING)
    with Session(e) as session:
        exe = session.connection().execute(sqla.text(f'SELECT COUNT(*) FROM {table_name}'))
        result = exe.scalar()

    # return the result
    return result
