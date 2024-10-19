from pathlib import Path
import sys
from finance.finance_extractor import load, load_from_sqlite
from finance.finance_extractor import stage
from finance.finance_extractor import convert
from finance.finance_extractor import FileStager
from finance.finance_listener import ComptesModifiedHandler
from finance.finance_salaries import SalaryExtractor
from watchdog.observers import Observer
import finance.output as o
from time import sleep


def get_helpstring():
    """ Function that produces the help doc"""
    t = """No argument was given for the program.
            Please use following arguments : 
            -s  :   stage the files to load
            -c  :   convert the files to CSV
            -l :   load the files into the database
            -sqlite : load from SQLite into the database
            -salaires : load the salaries
            -listen : start the listener feature
            -h  :   list the help"""
    return t


def main(args=None, config_file: Path = None):
    """ Running the extraction program """

    if args is None:
        args = sys.argv[1:]
        run_stage = False
        run_convert = False
        run_load = False
        run_load_sqlite = False
        run_salaries = False
        run_listener = False

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
                elif param == '-listen':
                    run_listener = True
                elif param == '-scl':
                    # Extract and merge all at once
                    run_stage = True
                    run_convert = True
                    run_load = True
                elif param == '-sqlite':
                    run_load_sqlite = True
                else:
                    print(get_helpstring())
        except IndexError:
            print(get_helpstring())

        if run_listener:
            fs = FileStager()
            event_handler = ComptesModifiedHandler()
            observer = Observer()
            observer.schedule(event_handler, path=fs.get_staging_folder(), recursive=False)
            observer.start()
            o.print_title('Starting Listener')
            try:
                while True:
                    o.print_event(f'listening')
                    sleep(2)
            except KeyboardInterrupt:
                observer.stop()

            observer.join()
        else:
            if run_stage:
                o.print_title('Running staging mechanism')
                stage()

            if run_convert:
                o.print_title('Running conversion mechanism')
                convert()

            if run_load:
                o.print_title('Running loading mechanism')
                load()
            if run_load_sqlite:
                o.print_title('Running loading from SQLite')
                load_from_sqlite()

            if run_salaries:
                o.print_title('Running salaries loading')
                se = SalaryExtractor()
                f = se.get_source_file()
                wkb = se.get_spreadsheet(f)
                sheet = se.get_salary_sheet(wkb)
                salaires = se.parse_salary_sheet(sheet)
                df = se.convert_salaries_to_dataframe(salaires)
                se.save_dataframe_to_sql(df)


if __name__ == '__main__':
    main()
