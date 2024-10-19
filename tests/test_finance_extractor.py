from unittest import TestCase
from finance.finance_extractor import FileLoader, FileConverter, DatabaseConverter
import pandas as pd
import numpy as np
from pathlib import Path


def generate_dataframe_with_zeroes() -> pd.DataFrame:
    np.random.seed(42)

    # Generate random numbers with some zeroes
    num_rows = 10  # Number of rows in the DataFrame
    data = {
        'Column1': np.random.randint(0, 10, size=num_rows),
        'Column2': np.random.randint(0, 10, size=num_rows)
    }

    # Create the DataFrame*
    df = pd.DataFrame(data)
    df.loc[(df['Column1'].index % 2 == 0), 'Column1'] = 0
    return df


class TestDatabaseLoad(TestCase):
    def test_1_replace_zeroes(self):
        test_df = generate_dataframe_with_zeroes()
        # count the number of zeroes in the first column
        nb_zeroes = (test_df['Column1'] == 0).sum()
        # replace the zeroes
        fl = FileLoader(['Column1', 'Column2'])
        test_df['Column1'] = fl.replace_zeroes_with_null(test_df['Column1'])
        nb_null = test_df['Column1'].isnull().sum()
        self.assertGreater(nb_zeroes, 0, 'There are no zero values in the test dataset !')
        self.assertGreater(nb_null, 0, 'There are no null values in the test dataset !')

    def test_2_clean_categories(self):
        s = pd.Series(['Hello', 'good Bye', None], name='Cat')
        print(s)
        fl = FileLoader(['Colonne 1', 'Colonne 2'])
        s = fl.clean_categories(s)
        print(s)
        print(s.isna())

    def test_3_convert_dates(self):
        # Load the dataframe
        fl = FileLoader(['Date'])
        df = fl.load_dataframe_from_csv(Path('Comptes_2013.csv'))
        self.assertIsNotNone(df, 'Could not load the dataframe')
        self.assertGreater(len(df), 0, 'Dataframe is empty')
        df = df[df['Date'] != '9999-12-31']
        newdatecolumn = fl.parse_date(df['Date'])
        self.assertTrue(True, 'No error shown')

    def test_4_extract_dates(self):
        # Load the dataframe
        fl = FileLoader(['Date'])
        df = fl.load_dataframe_from_csv(Path('Comptes_2013.csv'))
        self.assertIsNotNone(df, 'Could not load the dataframe')
        self.assertGreater(len(df), 0, 'Dataframe is empty')
        df = df[df['Date'] != '9999-12-31']
        newdatecolumn = fl.check_correct_date_format(df['Date'])
        self.assertEqual(newdatecolumn.sum(), 6, 'Je ne retrouve pas les 6 valeurs de date fausses')

    def test_5_filter_wrong_dates(self):
        fl = FileLoader(['Date'])
        df = fl.load_dataframe_from_csv(Path('Comptes_2013.csv'))
        self.assertIsNotNone(df, 'Could not load the dataframe')
        self.assertGreater(len(df), 0, 'Dataframe is empty')
        df = fl.check_wrong_dates_in_data_frame(df)
        self.assertEqual(len(df), 6, 'Le dataframe filtré ne contient pas les 6 fausses valeurs')

class TestDatabaseExtract(TestCase):
    def test_get_transactions(self):
        fc = DatabaseConverter()
        df = fc.get_transactions()

        self.assertGreater(len(df), 0, 'No rows founds')


