"""
We first define the main function
"""
import sys

import finance.help as fh
import finance.extract_comptes as fe
import finance.merge_comptes as fm

def main(args=None):
    """ Running the extraction program """

    if args is None:
        args = sys.argv[1:]

        try:
            param = args[0]
            if param == '-e':
                # Run the extract method
                fe.run_extractor()
            elif param == '-m':
                # Run the merge method
                fm.run_merger()
            elif param == '-em':
                # Extract and merge all at once
                fe.run_extractor()
                fm.run_merger()
            else:
                print(fh.get_helpstring())
        except IndexError:
            print(fh.get_helpstring())


# and now, the context checker
if __name__ == "__main__":
    main()