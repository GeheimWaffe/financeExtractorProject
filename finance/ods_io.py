from odf.opendocument import OpenDocumentSpreadsheet
from odf.opendocument import load
from odf.element import Element
from odf.table import Table
from odf.table import TableRow
from odf.table import TableCell
from odf.text import P
from pathlib import Path
import datetime as dt
import pandas as pd

def generate_cell_empty(stylename: str = '', rule: str = '') -> Element:
    result = TableCell()
    if stylename != '':
        result.setAttribute('stylename', stylename)
    if rule != '':
        result.setAttribute('contentvalidationname', rule)
    return result

def generate_table_cell_text(content: str, stylename: str = '', rule: str = '') -> Element:
    result = TableCell(valuetype='string')
    if stylename != '':
        result.setAttribute('stylename', stylename)
    if rule != '':
        result.setAttribute('contentvalidationname', rule)
    result.addElement(P(text=content))
    return result


def generate_table_cell_float(content: float, stylename: str = '', rule: str = '') -> Element:
    result = TableCell(valuetype='float', value=str(content))
    if stylename != '':
        result.setAttribute('stylename', stylename)
    if rule != '':
        result.setAttribute('contentvalidationname', rule)
    result.addElement(P(text=str(content)))
    return result

def generate_table_cell_datetime(content: dt.datetime, stylename: str = '', rule: str = '') -> Element:
    result = TableCell(valuetype='date', datevalue=content.strftime('%Y-%m-%dT%H:%M:%S'))
    if stylename != '':
        result.setAttribute('stylename', stylename)
    if rule != '':
        result.setAttribute('contentvalidationname', rule)
    if content.hour + content.minute + content.second == 0:
        result.addElement(P(text=content.strftime('%d/%m/%Y')))
    else:
        result.addElement(P(text=content.strftime('%d/%m/%Y %H:%M')))
    return result

class CellWrapper:
    __cell__: Element

    def __init__(self, cell: Element):
        self.__cell__ = cell

    @property
    def element(self) -> Element:
        return self.__cell__

    def get_cell_column_span(self) -> int:
        try:
            span = self.__cell__.getAttribute('numbercolumnsrepeated')
            return 1 if span is None else int(span)
        except ValueError:
            return 1

    def get_cell_value(self) -> any:
        if self.__cell__.hasChildNodes():
            return str(self.__cell__.childNodes[0])
        else:
            return ''

    def get_element_style(self) -> str:
        style = self.__cell__.getAttribute('stylename')
        return '' if style is None else style


    def get_cell_validation(self) -> str:
        rule = self.__cell__.getAttribute('contentvalidationname')
        return '' if rule is None else rule


class RowWrapper:
    __element__: Element
    __stylename__: str

    def __init__(self, stylename: str = ''):
        self.__element__ = TableRow()
        if stylename != '':
            self.__stylename__ = stylename
            self.__element__.setAttribute('stylename', stylename)

    @property
    def element(self) -> Element:
        return self.__element__

    @element.setter
    def element(self, elt: Element):
        self.__element__ = elt

    def is_row_empty(self) -> bool:
        analysis = [c.hasChildNodes() for c in self.__element__.childNodes]
        return not any(analysis)

    def get_cell_count(self) -> int:
        result = 0
        for e in self.__element__.getElementsByType(TableCell):
            c = CellWrapper(e)
            result += c.get_cell_column_span()
        return result

    def get_values(self) -> list:
        """ iterates over all the cells and retrieves the value array"""
        result = []
        for e in self.__element__.getElementsByType(TableCell):
            c = CellWrapper(e)
            span = c.get_cell_column_span()
            result += [c.get_cell_value()] * span
        return result

    def get_value(self, column: int) -> any:
       if column < len(self.__values__):
           return self.__values__[column]

    def get_cell_style(self, index: int) -> str:
        max_index = 0
        for e in self.__element__.getElementsByType(TableCell):
            c = CellWrapper(e)
            max_index += c.get_cell_column_span()
            if index < max_index:
                if c.get_element_style() == '':
                    return self.__stylename__
                else:
                    return c.get_element_style()

    def get_cell_validation(self, index: int) -> str:
        max_index = 0
        for e in self.__element__.getElementsByType(TableCell):
            c = CellWrapper(e)
            max_index += c.get_cell_column_span()
            if index < max_index:
                return c.get_cell_validation()


class SheetWrapper:
    __sheet__: Table

    def __init__(self, value: Table):
        self.__sheet__ = value

    @property
    def name(self) -> str:
        return self.__sheet__.getAttribute('name')

    def get_row(self, index: int) -> Element:
        return self.__sheet__.getElementsByType(TableRow)[index]

    def get_row_count(self) -> int:
        return len(self.__sheet__.getElementsByType(TableRow))


    def insert_from_array(self, table_of_values: list):
        for value_row in table_of_values:
            row = TableRow()
            for value in value_row:
                if isinstance(value, float):
                    row.addElement(generate_table_cell_float(value))
                else:
                    row.addElement(generate_table_cell_text(str(value)))
            # add the row to the table
            self.__sheet__.addElement(row)

    def insert_from_dataframe(self, df: pd.DataFrame, include_headers: bool = False, mode: str = 'overwrite'):
        """ inserts a pandas dataframe into the sheet.

        :param df: the dataframe to insert
        :param include_headers: True to include the dataframe headers in the import
        :param mode: the insertion mode.
            'overwrite' : erases all the rows and writes from scratch in the sheet.
            'append' : finds the first empty row and then appends."""

        empty_row = None
        if mode == 'overwrite':
            # delete all the rows
            rows = self.__sheet__.getElementsByType(TableRow)
            for r in rows:
                self.__sheet__.removeChild(r)
        elif mode == 'append':
            # find the first empty row
            rows = self.__sheet__.getElementsByType(TableRow)
            for r in rows:
                rw = RowWrapper()
                rw.element = r
                if rw.is_row_empty():
                    empty_row = rw
                    break

        # create the headers
        if include_headers:
            row = TableRow()
            for c in df.columns:
                row.addElement(generate_table_cell_text(c))
            self.__sheet__.insertBefore(row, empty_row)

        # import the values
        for df_row in df.itertuples(index=False):
            row = TableRow()
            for i in range(len(df.columns)):
                style = empty_row.get_cell_style(i)
                rule = empty_row.get_cell_validation(i)
                if df.dtypes[i] == 'float64':
                    row.addElement(generate_table_cell_float(df_row[i], style, rule))
                elif df.dtypes[i] == r'datetime64[ns]':
                    row.addElement(generate_table_cell_datetime(df_row[i], style, rule))
                else:
                    row.addElement(generate_table_cell_text(df_row[i], style, rule))

            # if there are extra styles, create empty cells
            if empty_row.get_cell_count() > len(df.columns):
                for i in range(len(df.columns), empty_row.get_cell_count()):
                    row.addElement(generate_cell_empty(empty_row.get_cell_style(i),
                                                       empty_row.get_cell_validation(i)))

            # add the row to the table
            self.__sheet__.insertBefore(row, empty_row.element)


class SpreadsheetWrapper:
    __workbook__: OpenDocumentSpreadsheet
    __sheets__ = {}

    def __init__(self):
        self.__workbook__ = OpenDocumentSpreadsheet()

    def load(self, filepath: Path):
        self.__workbook__ = load(filepath)
        for t in self.__workbook__.getElementsByType(Table):
            sheet = SheetWrapper(t)
            self.__sheets__[sheet.name] = sheet

    def to_xml(self):
        self.__workbook__.toXml('odsxml.xml')

    def save(self, filepath: Path):
        self.__workbook__.write(filepath)

    def get_sheets(self) -> dict:
        """ returns the dictionary of the sheets"""
        return self.__sheets__
