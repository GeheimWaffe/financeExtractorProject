"""
We first define the main function
"""
import shutil
import pandas as pd
import pyexcel_ods3
from datetime import datetime

import sqlalchemy

from finance.database import load_to_table
from finance.database import get_row_count
import finance.output as o
import re

from pathlib import Path


class FileStager:
    """This class has the responsibility for picking the right files and stage them"""
    __staging_folder__ = Path.home().joinpath('Comptes')
    """
    The staging folder is where all the CSV files are stored
    for loading into the database.
    checks if the target extract folder exists and if not, creates it
    """

    def create_staging_folder(self) -> bool:
        try:
            self.__staging_folder__.mkdir()
            return True
        except FileExistsError:
            return False

    def sweep_and_push(self, folder: Path, mask: str, targetfolder: Path) -> int:
        files = [p for p in folder.iterdir() if p.name.find(mask) > -1]
        for f in files:
            o.print_event(f'* file found : {f}')
            shutil.copy2(f, targetfolder)
            o.print_event(f'* file copied in target folder : {targetfolder}')

        return len(files)

    def is_valid_file(self, filename: str) -> bool:
        mask = r'^Comptes.*'
        return bool(re.match(mask, filename))

    def get_source_files(self):
        p: Path
        return [p for p in self.__staging_folder__.iterdir() if p.is_file() and self.is_valid_file(p.name)]

    def get_staging_folder(self):
        return self.__staging_folder__


class FileConverter:
    """This class has the responsibility for converting arrays of ODS files to csv"""
    __extract_folder__ = Path.home().joinpath('Extracts')

    def get_extract_folder(self) -> Path:
        return self.__extract_folder__

    def get_converted_files(self):
        return self.__extract_folder__.iterdir()

    def convert_account_file(self, p: Path) -> pd.DataFrame:
        """ This class converts an account file"""
        data = pyexcel_ods3.get_data(str(p))

        # get the sheet
        mouvements_sheet = data['Mouvements']
        o.print_event('sheet retrieved')

        # find the first row of the headers
        r = 0
        while mouvements_sheet[r][0] != 'Date':
            r += 1

        # extract the table
        mouvements: []
        mouvements = mouvements_sheet[r + 1::]

        headers: []
        headers = mouvements_sheet[r:r + 1:]

        # create a pandas dataframe
        df = pd.DataFrame(mouvements, columns=headers)
        o.print_event('pandas Dataframe created')
        return df

    def save_dataframe(self, df: pd.DataFrame, csv_name: str):
        # save the dataframe
        df.to_csv(self.__extract_folder__.joinpath(csv_name + '.csv'))


class DatabaseConverter():
    """ Class which extracts from the SQLite database the dataframe"""
    __connection_string__ = 'sqlite+pysqlite:///' + Path().home().joinpath('finance.sqlite').as_posix()

    def get_transactions(self) -> pd.DataFrame:
        """ This class loads from the sqlite database"""
        e = sqlalchemy.create_engine(self.__connection_string__)
        with e.connect() as conn:
            df = pd.read_sql_table('comptes', conn)

        df.set_index('index', inplace=True)
        # Add a date checker column
        df['Date Out of Bound'] = df['Date'].dt.year > df['File Year']

        return df


class FileLoader:
    """ class for loading csv files into the Postgresql database"""
    __table_comptes__ = 'comptes'
    __acceptables_columns = ()

    def __init__(self, acceptable_columns: set):
        self.__acceptables_columns = acceptable_columns

    def replace_euros_in_column(self, col: pd.Series) -> pd.Series:
        """ Removes the euros and converts to numeric"""
        result = col.str.replace(' EUR', '')
        result = pd.to_numeric(result)
        return result

    def clean_boolean_columns(self, col: pd.Series) -> pd.Series:
        """ cleans up a boolean column"""
        result = col.astype(str)
        result = result.replace({'nan': 'false', 'True': 'true', 'False': 'false', '0': 'false', '1': 'true'})
        return result

    def clean_categories(self, col: pd.Series) -> pd.Series:
        """ cleans up the categories by making them unified"""
        result: pd.Series
        result = col.astype(str)
        result = result.str.title()
        return result

    def check_correct_date_format(self, col: pd.Series) -> pd.Series:
        """ verifies the columns"""
        result = col.astype(str)
        result = result.apply(lambda date_str: bool(re.match(r'\d{4}-\d{2}-\d{2}\s.+', date_str)))
        return result

    def parse_date(self, col: pd.Series) -> pd.Series:
        """ parses the column to a date"""
        result: pd.Series
        result = pd.to_datetime(col, format='%Y-%m-%d', exact=True)
        return result

    def replace_zeroes_with_null(self, col: pd.Series) -> pd.Series:
        result: pd.Series
        result = col.replace(0, None)
        return result

    def get_fileyear(self, p: Path) -> int:
        """
        calculates the year of a comptes extract. Expectation : the file should be named Comptes YYYY
        :param p: the path of the CSV file.
        :return: the year
        """
        filename = p.stem
        year = int(filename[-4:])
        return year

    def load_dataframe_from_csv(self, csv_file: Path) -> pd.DataFrame:
        if csv_file.suffix == '.csv':
            # this is a CSV, we can go for it
            df = pd.read_csv(csv_file, parse_dates=True)

            # extract the year
            year = self.get_fileyear(csv_file)

            # add a new column
            df['File Year'] = year
            # end
            return df

    def check_wrong_dates_in_data_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Filters the dataframe over the wrong date columns """
        wrong_dates = self.check_correct_date_format(df['Date'])
        df = df[wrong_dates]
        return df

    def keep_only_acceptable_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # first we select the right columns
        droppable_columns = [x for x in df.columns if x not in self.__acceptables_columns]
        return df.drop(columns=droppable_columns)

    def cleanup_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
            Cleans up the global data frame
            :param df: the data frame to be cleaned up
            :return: a cleaned up data frame
            """
        df = self.keep_only_acceptable_columns(df)

        # Then we convert to proper numeric types the Dépenses and Recettes
        df['Dépense'] = self.replace_euros_in_column(df['Dépense'])
        df['Recette'] = self.replace_euros_in_column(df['Recette'])
        df['Provision à payer'] = self.replace_euros_in_column(df['Provision à payer'])
        df['Provision à récupérer'] = self.replace_euros_in_column(df['Provision à récupérer'])

        # Clean up the booleans
        df['Economie'] = self.clean_boolean_columns(df['Economie'])
        df['Réglé'] = self.clean_boolean_columns(df['Réglé'])

        # Clean up the dates
        df = df[df['Date'] != '9999-12-31']
        df['Date'] = self.parse_date(df['Date'])

        # clean up the month
        df['Mois'] = self.parse_date(df['Mois'])

        # clean up the catégories
        df['Catégorie'] = self.clean_categories(df['Catégorie'])

        # Add a date checker column
        df['Date Out of Bound'] = df['Date'].dt.year > df['File Year']

        # Calculate the provision à récupérer
        df.reset_index(drop=True, inplace=True)
        df.loc[~df['Taux de remboursement'].isna(), 'Provision à récupérer'] = df['Dépense'] * df[
            'Taux de remboursement']

        # Rename the columns
        df.rename(columns={'N°': 'No', 'N° de référence': 'no_de_reference', 'Fait Marquant': 'fait_marquant',
                           'Taux de remboursement': 'taux_remboursement'}, inplace=True)

        return df

    def save_dataframe_to_sql(self, df: pd.DataFrame) -> int:
        """
        Save the data frame to the PostGres database
        """

        rows = load_to_table('comptes', df)
        rows = get_row_count('comptes')
        return rows

    def save_dataframe_to_csv(self, df: pd.DataFrame, filename: str, append_timestamp: bool) -> bool:
        """
        Save the dataframe to a CSV file in my home folder
        :param df: the data frame to save
        :param filename: the file name pattern
        :param append_timestamp: to indicate if a timestamp is to be appended
        :return: success of the operation
        """
        # loop over. Save the data frame
        if append_timestamp:
            global_filepath = Path.home().joinpath(
                ''.join([filename, datetime.now().strftime('%Y-%m-%d %H-%M-%S'), '.csv']))
        else:
            global_filepath = Path.home().joinpath(
                ''.join([filename, ' ', '.csv']))
        return True


def stage():
    o.print_title("Starting Finance Extractor...")

    o.print_title("Staging")
    fs = FileStager()
    ct = fs.sweep_and_push(Path.home().joinpath('Bureau'), '^Comptes.*ods$', fs.get_staging_folder())
    o.print_event(f'{ct} files found and pushed to the staging area')


def convert():
    o.print_title("Iterating over the source files")
    fs = FileStager()
    files = fs.get_source_files()
    o.print_event(f'{len(files)} files found')

    # iterate over each file
    fc = FileConverter()
    p: Path
    df: pd.DataFrame
    for p in files:
        # calling the read method
        o.print_event(f'extracting the file : {p}')
        df = fc.convert_account_file(p)
        o.print_event(f'dataframe created')
        o.print_event(f'columns : {df.columns}')
        o.print_event(f'rows : {len(df)}')
        # save the dataframe
        fc.save_dataframe(df, p.stem)
        o.print_event('Output saved')


def load_files_without_clean() -> pd.DataFrame:
    o.print_title('Loading files')
    fc = FileConverter()
    o.print_event(f'* scanning folder : {fc.get_extract_folder()}')
    files = fc.get_converted_files()
    files = [f for f in files if f.suffix == '.csv']
    o.print_event(f'{len(files)} files found')

    # define acceptable columns
    acceptable_columns = ('Catégorie', 'Compte', 'Date', "Date d'insertion",
                          'Description', 'Dépense', 'Economie',
                          'File Year', 'Mois', 'Recette', 'Réglé',
                          'Provision à payer', 'Provision à récupérer',
                          'Date remboursement', 'Organisme')

    fl = FileLoader(acceptable_columns)
    p: Path
    df: pd.DataFrame
    dataframes = []
    for p in files:
        o.print_event(f'loading file : {p}')
        # load the dataframe
        df = fl.load_dataframe_from_csv(p)
        o.print_event(f'file loaded : {len(df)} rows')
        dataframes.append(df)

    if len(dataframes) > 0:
        # global merge
        global_df = pd.concat(dataframes)
    else:
        global_df = None

    return global_df


def load():
    o.print_title('Loading files')
    fc = FileConverter()
    o.print_event(f'* scanning folder : {fc.get_extract_folder()}')
    files = fc.get_converted_files()
    files = [f for f in files if f.suffix == '.csv']
    o.print_event(f'{len(files)} files found')

    # define acceptable columns
    acceptable_columns = ('Date', 'N°', 'Description', 'Dépense', 'N° de référence', 'Recette',
                          'Taux de remboursement', 'Compte', 'Catégorie',
                          'Economie', 'Réglé', 'Mois', "Date d'insertion",
                          'Provision à payer', 'Provision à récupérer',
                          'Date remboursement', 'Organisme', 'Fait Marquant', 'File Year')

    fl = FileLoader(acceptable_columns)
    p: Path
    df: pd.DataFrame
    dataframes = []
    for p in files:
        o.print_event(f'loading file : {p}')
        # load the dataframe
        df = fl.load_dataframe_from_csv(p)
        o.print_event(f'file loaded : {len(df)} rows')
        dataframes.append(df)

    # global cleanup
    if len(dataframes) > 0:
        # global merge
        global_df = pd.concat(dataframes)
        o.print_event(f'dataframes merged : {len(global_df)} rows in total')
        o.print_event(f'checking the date formats...')
        wrong_dates = fl.check_wrong_dates_in_data_frame(global_df)
        if len(wrong_dates) > 0:
            o.print_event(f'wrong dates found !')
            o.print_event(wrong_dates[['Date', 'File Year']])
        else:
            global_df = fl.cleanup_dataframe(global_df)
            o.print_event(f'global dataframe cleaned up')
            loaded_rows = fl.save_dataframe_to_sql(global_df)
            o.print_event(f'dataframe loaded : {loaded_rows} loaded')
    else:
        o.print_event(f'no dataframes found')


def load_from_sqlite():
    o.print_title('Loading from SQLite database')
    dc = DatabaseConverter()
    o.print_event('Retrieving the SQLite content')
    df = dc.get_transactions()
    o.print_event('Loading to Postgres Database')
    # define acceptable columns
    acceptable_columns = ('Date', 'Description', 'Recette', 'Dépense',
                          'Compte', 'Catégorie', 'Economie', 'Réglé',
                          'Mois', "Date d'insertion", 'File Year',
                          'Provision à payer', 'Provision à récupérer',
                          'Date remboursement', 'Organisme', 'Date Out of Bound',
                          'taux_remboursement', 'fait_marquant', 'No',
                          'no_de_reference')

    fl = FileLoader(acceptable_columns)

    loaded_rows = fl.save_dataframe_to_sql(df)
    o.print_event(f'dataframe loaded : {loaded_rows} loaded')
