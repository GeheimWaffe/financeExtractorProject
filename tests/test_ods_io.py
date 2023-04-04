from unittest import TestCase
from finance.ods_io import SpreadsheetWrapper
from finance.ods_io import SheetWrapper
from finance.ods_io import RowWrapper
from pathlib import Path
class TestSpreadsheetWrapper(TestCase):
    __sh__ = None

    def get_workbook(self, p: Path) -> SpreadsheetWrapper:
        if self.__sh__ is None:
            self.__sh__ = SpreadsheetWrapper()
            self.__sh__.load(p)

        return self.__sh__

    def test_load(self):
        p = Path.home().joinpath('Comptes', 'Comptes_2023.ods')
        sh = self.get_workbook(p)
        sheets = sh.get_sheets()['Salaires']
        self.assertIsNotNone(sheets, 'Could not find Salaires sheet')

    def test_read_sheet_row(self):
        p = Path.home().joinpath('Comptes', 'Comptes_2023.ods')
        sh = self.get_workbook(p)
        sheet: SheetWrapper
        sheet = sh.get_sheets()['Salaires']
        print(f'row count : {sheet.get_row_count()}')
        self.assertGreater(sheet.get_row_count(), 0, '0 rows found')

    def test_get_specific_cell(self):
        p = Path.home().joinpath('Comptes', 'Comptes_2023.ods')
        sh = self.get_workbook(p)
        sheet: SheetWrapper
        sheet = sh.get_sheets()['Salaires']
        row = sheet.get_row(2)
        rwrap = RowWrapper()
        rwrap.element = row
        values = rwrap.get_values()
        self.assertGreater(sheet.get_row_count(), 0, '0 rows found')


