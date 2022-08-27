# The goal of this project is to extract from a Comptes files a CSV table containing all the data of
# a Comptes files.
# This data should be extracted to a target folder.
# After that, I will have a set of CSV files
# Then, another method should
# 1. Extract the content of each file
# 2. Append it to a global data frame
# 3. Save the dataframe to a global CSV file
# 4. push the extracted file to a subfolder
import pathlib

import pandas as pd
import pyexcel_ods3
import finance.FileSystem as fs

def create_extract_folder():
    """
    checks if the target extract folder exists and if not, creates it
    :return: result of the creation process
    """
    p = pathlib.Path.home().joinpath(fs.EXTRACT_FOLDER)

    if not p.exists():
        p.mkdir()
        return False
    else:
        return True


def get_ods_files(source_folder: str) -> []:
    """
    get a list of all files to convert
    :param source_folder: the source folder, located in the HOME folder
    :return: a list of Path objects representing each source file
    """
    # define the source folder path
    p = pathlib.Path.home().joinpath(source_folder)

    return p.iterdir()


def read_account_file(p: pathlib.Path, target_folder: str):
    """ read and extract from my Comptes file

    :param p: the path of the file
    :param target_folder:the folder where to store the file
    :return: nothing
    """
    if p.is_file():

        data = pyexcel_ods3.get_data(str(p))

        # get the sheet
        mouvements_sheet = data['Mouvements']
        print('** sheet retrieved **')

        # find the first row of the headers
        r = 0
        while mouvements_sheet[r][0] != 'Date':
            r += 1

        # extract the table
        mouvements: []
        mouvements = mouvements_sheet[r+1::]

        headers: []
        headers = mouvements_sheet[r:r+1:]

        # create a pandas dataframe
        df = pd.DataFrame(mouvements, columns=headers)
        print('** pandas Dataframe created **')

        # create the CSV name
        csv_name = p.stem

        # save the dataframe
        df.to_csv(p.parent.parent.joinpath(target_folder, csv_name + '.csv'))
        print('** Output saved **')
    else:
        pass  # this is a sub-folder, we don't do anything


def run_extractor():
    print("*** Starting Finance Extractor... ***")

    print("* Check if folder exists")
    if create_extract_folder():
        print("* Extract folder created")
    else:
        print("* Extract folder already present")

    print("** Iterating over the source files")
    files = get_ods_files(fs.SOURCE_FOLDER)
    print(f'** files found')

    # iterate over each file
    p: pathlib.Path

    for p in files:
        # calling the read method
        print(f'extracting the file : {p}')
        read_account_file(p, fs.EXTRACT_FOLDER)

    print("*** Finance Extractor Program executed ***")