# produces the help file

def get_helpstring():
    """ Function that produces the help doc"""
    t = """No argument was given for the program.
            Please use following arguments : 
            -e  :   extract the content of the ODS files
            -m  :   merge the extracted CSVs and inject in the database
            -em :   extract and merge in one step
            -h  :   list the help"""
    return t