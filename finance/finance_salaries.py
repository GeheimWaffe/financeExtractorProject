# module dedicated to salary extraction and loading into the database
from pathlib import Path

import pandas as pd
import pyexcel_ods3
import ods_io

import datetime as dt
from finance.database import load_to_table
from finance.database import get_row_count

def print_log(message: str):
    """ central function for printing console messages"""
    print('*** Salary extraction : ' + message)

class SalaryExtractor:
    __root_folder__ = 'Comptes'
    __salary_sheet__ = 'Salaires'
    """ This class is dedicated to salary extraction"""

    def get_source_file(self) -> Path:
        """ retrieve the selected file"""
        for f in Path.home().joinpath(self.__root_folder__).iterdir():
            if f.suffix == '.ods' and f.name[0] != '~':
                return f

    def get_spreadsheet(self, source_file: Path) -> ods_io.SpreadsheetWrapper:
        """ This class converts an account file"""
        sh = ods_io.SpreadsheetWrapper()
        sh.load(source_file)
        return sh

    def get_salary_sheet(self, wkb: ods_io.SpreadsheetWrapper) -> ods_io.SheetWrapper:
        sh = wkb.get_sheets()['Salaires']
        return sh

    def parse_salary_sheet(self, sheet: ods_io.SheetWrapper) -> list:
        """ This method parses the salary sheet and converts it to a table with
        Item
        Month
        Value"""
        pivot_x = 3
        pivot_y = 1
        result = []
        print_log(f'setting up the pivot cell : ({pivot_x}, {pivot_y}')

        # parse the dates
        print_log('retrieving the header row')
        date_row = ods_io.RowWrapper()
        date_row.element = sheet.get_row(pivot_y)
        print_log(f'header row retrieved : {date_row.get_cell_count()} found')
        # SELECT THE DATES
        dates = date_row.get_values()
        print_log(f'values retrieved : {len(dates)} found')

        # find the first column
        print_log('searching for the first date-like header')
        y_start = 0
        while dates[y_start] == '':
            y_start += 1
        print_log(f'first date-like header found at position {y_start}')

        months = dates[y_start:]
        month_length = len(months)
        print_log(f'number of months : {month_length}')

        # parse the content
        rw = ods_io.RowWrapper()
        row_num = sheet.get_row_count()
        print_log(f'sheet opened, number of rows : {row_num}')
        result = []
        # iterate over the rows
        for i in range(row_num):
            print_log(f'reading row {i}')
            rw.element = sheet.get_row(i)
            values = rw.get_values()
            count_values = len(values)
            print_log(f'{count_values} rows found')
            if len(values) > pivot_y:
                header = values[pivot_y]
                category = values[pivot_y-1]
                print_log(f'header found : {header}, category : {category}')
                if header != '':
                    for j in range(y_start,count_values):
                        if j < len(values):
                            result += [[category, header, dates[j], str(values[j]).replace(' €', '')]]

                    print(f'{len(values)} found')
        return result

    def convert_salaries_to_dataframe(self, salaries:list) -> pd.DataFrame:
        print_log('creating the salaries dataframe')
        df = pd.DataFrame(salaries, columns=['Catégorie', 'Poste', 'Mois', 'Valeur'])
        print_log('converting the valeur to a numeric value')
        df['Valeur Numérique'] = df['Valeur'].replace('\.', '', regex=True).replace(',', '.', regex=True)
        df['Valeur Numérique'] = pd.to_numeric(df['Valeur Numérique'])
        print_log('conversion done')
        return df

    def save_dataframe_to_sql(self, df: pd.DataFrame) -> int:
        """
        Save the data frame to the PostGres database
        """
        rows = load_to_table('salaires', df)
        rows = get_row_count('salaires')
        return rows