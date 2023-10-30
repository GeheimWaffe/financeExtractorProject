from unittest import TestCase
import finance.finance_salaries as fs

class TestSalaryExtract(TestCase):
    def test_comptes_file(self):
        se = fs.SalaryExtractor()
        comptes_files = se.get_source_file()

        self.assertIsNotNone(comptes_files, 'No Comptes files found')

    def test_salary_extract(self):
        se = fs.SalaryExtractor()
        wkb = se.get_spreadsheet(se.get_source_file())
        salaries = se.get_salary_sheet(wkb)
        self.assertGreater(len(salaries), 0, 'Salaries table is empty')
        self.assertEqual('Catégorie', salaries[2][0], 'Could not find first column header : Catégorie')
        self.assertEqual('Item', salaries[2][1], 'Could not find second column header : Item')

    def test_salary_parser(self):
        se = fs.SalaryExtractor()
        wkb = se.get_spreadsheet(se.get_source_file())
        salaries = se.get_salary_sheet(wkb)
        result = se.parse_salary_sheet(salaries)
        for r in result:
            print(r)
        self.assertIsNotNone(result)

    def test_salary_dataframe(self):
        se = fs.SalaryExtractor()
        wkb = se.get_spreadsheet(se.get_source_file())
        salaries = se.get_salary_sheet(wkb)
        data = se.parse_salary_sheet(salaries)
        df = se.convert_salaries_to_dataframe(data)
        print(df)
        self.assertIsNotNone(df)
