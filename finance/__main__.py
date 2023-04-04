from pathlib import Path
import sys
from finance.finance_extractor import load
from finance.finance_extractor import stage
from finance.finance_extractor import convert
from finance_salaries import SalaryExtractor

def get_helpstring():
    """ Function that produces the help doc"""
    t = """No argument was given for the program.
            Please use following arguments : 
            -s  :   stage the files to load
            -c  :   convert the files to CSV
            -l :   load the files into the database
            -h  :   list the help"""
    return t

def main(args=None, config_file:Path = None):
    """ Running the extraction program """

    if args is None:
        args = sys.argv[1:]
        run_stage = False
        run_convert = False
        run_load = False
        run_salaries = False

        try:
            for param in args:
                if param == '-s':
                    # Run the staging method
                    run_stage = True
                elif param == '-c':
                    # Run the convert method
                    run_convert = True
                elif param == '-l':
                    # run the load method
                    run_load = True
                elif param == '-salaires':
                    run_salaries = True
                elif param == '-scl':
                    # Extract and merge all at once
                    run_stage = True
                    run_convert = True
                    run_load = True
                else:
                    print(get_helpstring())
        except IndexError:
            print(get_helpstring())

        if run_stage:
            print('*** Running staging mechanism ***')
            stage()

        if run_convert:
            print('*** Running conversion mechanism ***')
            convert()

        if run_load:
            print('*** Running loading mechanism')
            load()

        if run_salaries:
            print('*** Running salaries loading')
            se = SalaryExtractor()
            f = se.get_source_file()
            wkb = se.get_spreadsheet(f)
            sheet = se.get_salary_sheet(wkb)
            salaires = se.parse_salary_sheet(sheet)
            df = se.convert_salaries_to_dataframe(salaires)
            se.save_dataframe_to_sql(df)


if __name__ == '__main__':
    main()