# this file enables to save the data frame to my database instance

import pandas as pd
import sqlalchemy as sqla
import pathlib
from datetime import datetime

def create_engine() -> sqla.engine.Engine:
    return  sqla.create_engine('postgresql://regular_user:user_password@localhost:5432/finance')


def save_dataframe_to_sql(df: pd.DataFrame, tablename:str) -> bool:
    """
    Save the data frame to the PostGres database
    :param df: the data frame to save
    :param tablename: the target table
    :return:
    """
    # create the engine
    e = create_engine()

    # Save the dataframe
    with e.connect() as conn:
        # Truncate the table
        conn.execute('TRUNCATE public.comptes')

        # Append the new data
        df.to_sql(tablename, conn, if_exists='append')
        return True

def save_dataframe_to_csv(df: pd.DataFrame, filename: str, append_timestamp: bool) -> bool:
    """
    Save the dataframe to a CSV file in my home folder
    :param df: the data frame to save
    :param filename: the file name pattern
    :param append_timestamp: to indicate if a timestamp is to be appended
    :return: success of the operation
    """
    # loop over. Save the data frame
    if append_timestamp:
        global_filepath = pathlib.Path.home().joinpath(
            ''.join([filename, datetime.now().strftime('%Y-%m-%d %H-%M-%S'), '.csv']))
    else:
        global_filepath = pathlib.Path.home().joinpath(
            ''.join([filename, ' ', '.csv']))
