from pathlib import Path
import sys
from finance.finance_extractor import load
from finance.finance_extractor import stage
from finance.finance_extractor import convert

def get_helpstring():
    """ Function that produces the help doc"""
    t = """No argument was given for the program.
            Please use following arguments : 
            -e  :   extract the content of the ODS files
            -m  :   merge the extracted CSVs and inject in the database
            -em :   extract and merge in one step
            -h  :   list the help"""
    return t

def main(args=None, config_file:Path = None):
    """ Running the extraction program """

    if args is None:
        args = sys.argv[1:]

        print(args)

        try:
            param = args[0]
            if param == '-s':
                # Run the staging method
                stage()
            elif param == '-c':
                # Run the convert method
                convert()
            elif param == '-l':
                # run the load method
                load()
            elif param == '-sc':
                # Extract and merge all at once
                stage()
                convert()
            else:
                print(get_helpstring())
        except IndexError:
            print(get_helpstring())

if __name__ == '__main__':
    main()