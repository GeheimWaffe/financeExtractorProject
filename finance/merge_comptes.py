# routines to merge the various Comptes CSV extracts

import pandas as pd
import pathlib
import finance.FileSystem as fs
import finance.persistent_layer as pl

TABLE_COMPTES = 'comptes'

column_list = {}
def get_fileyear(p: pathlib.Path) -> int:
    """
    calculates the year of a comptes extract. Expectation : the file should be named Comptes YYYY
    :param p: the path of the CSV file.
    :return: the year
    """
    filename = p.stem
    year = int(filename[-4:])
    return year

def cleanup_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans up the global data frame
    :param df: the data frame to be cleaned up
    :return: a cleaned up data frame
    """
    # first we select the right columns

    acceptable_columns = ('Catégorie', 'Compte', 'Date', "Date d'insertion",
                          'Description', 'Dépense', 'Economie',
                          'File Year', 'Mois', 'Recette', 'Réglé',
                          'Provision à payer', 'Provision à récupérer')

    droppable_columns = [x for x in df.columns if x not in acceptable_columns]

    df = df.drop(columns=droppable_columns)

    # Then we convert to proper numeric types the Dépenses and Recettes
    df['Dépense'] = replace_euros_in_column(df['Dépense'])
    df['Recette'] = replace_euros_in_column(df['Recette'])
    df['Provision à payer'] = replace_euros_in_column(df['Provision à payer'])
    df['Provision à récupérer'] = replace_euros_in_column(df['Provision à récupérer'])

    # Clean up the booleans
    df['Economie'] = clean_boolean_columns(df['Economie'])
    df['Réglé'] = clean_boolean_columns(df['Réglé'])

    # Clean up the dates
    df = df[df['Date'] != '9999-12-31']
    df['Date'] = pd.to_datetime(df['Date'])

    df['Date.Year'] = pd.DatetimeIndex(df['Date']).year
    df['Date.Month'] = pd.DatetimeIndex(df['Date']).month

    # clean up the month
    df['Mois'] = pd.to_datetime(df['Mois'])

    # Add a date checker column
    df['Date Out of Bound'] = df['Date.Year'] > df['File Year']

    return df


def replace_euros_in_column(col: pd.Series)-> pd.Series :
    """ Removes the euros and converts to numeric"""
    result = col.str.replace(' EUR', '')
    result = pd.to_numeric(result)
    return result


def clean_boolean_columns(col: pd.Series)-> pd.Series :
    """ cleans up a boolean column"""
    result = col.astype(str)
    result = result.replace({'nan': 'false', 'True': 'true', 'False': 'false', '0': 'false', '1': 'true'})
    return result


def run_merger():
    """
    Iterate over all the CSV files and merge them
    :return: 
    """
    print('*** Running the extract and merge operation')

    # fetch all the CSV files
    extract_path = pathlib.Path.home().joinpath(fs.EXTRACT_FOLDER)

    global_dataframe = pd.DataFrame()

    for p in extract_path.iterdir():
        print(f'*** File found : {str(p)}')
        if p.suffix == '.csv':
            # this is a CSV, we can go for it
            df = pd.read_csv(p, parse_dates=True)

            # extract the year
            year = get_fileyear(p)

            # add a new column
            df['File Year'] = year

            # append the data frame
            if len(global_dataframe.columns) == 0:
                global_dataframe = df
            else:
                global_dataframe = global_dataframe.append(df, sort=True)
                print("appended the data frame")

    # clean up the data frame
    global_dataframe = cleanup_dataframe(global_dataframe)
    print("*** Data frame has been cleaned up")

    # save the data frame
    import_result = pl.save_dataframe_to_sql(global_dataframe, TABLE_COMPTES)
    if import_result:
        print("*** Data frame saved to the PostgreSQL database")
    else:
        print('!!! Save operation unsuccessful')
