"""
This module is responsible for all Google sheets operations
"""
import re
from typing import List, Any

import pandas as pd
from pandas import DataFrame
import gspread

from hip_data_tools.common import LOG
from hip_data_tools.google.common import GoogleApiConnectionManager, GoogleApiConnectionSettings


class GoogleSheetConnectionManager(GoogleApiConnectionManager):
    """Encapsulates the Google sheets API connection settings"""

    def __init__(self, settings: GoogleApiConnectionSettings):
        super().__init__(settings, scope=['https://spreadsheets.google.com/feeds',
                                          'https://www.googleapis.com/auth/drive'])

    def get_client(self):
        """
        Get the connection for google sheets
        Returns: authorised connection for google sheets
        """
        return gspread.authorize(self._credentials())


def _get_value_matrix_from_range(worksheet, row_range):
    list_of_lists = []
    row_numbers = [int(i) for i in row_range.split(':')]
    first_row_number = row_numbers[0]
    last_row_number = row_numbers[1]
    for i in range(first_row_number, last_row_number + 1):
        row = worksheet.row_values(i)
        list_of_lists.append(row)
    return list_of_lists


def _get_value_matrix_from_skip(worksheet, data_start_row_number):
    list_of_lists = worksheet.get_all_values()
    del list_of_lists[0:data_start_row_number - 1]
    return list_of_lists


def _validate_field_names_and_types_count(field_names, field_types):
    if len(field_names) != len(field_types):
        LOG.error("Number of field names and number of field types are not matching")
        raise Exception("Field names and types are not matching")


def _validate_field_names(field_names):
    for field_name in field_names:
        # check for strings which only contains letters, numbers and underscores
        if not re.match("^[A-Za-z0-9_]+$", field_name):
            LOG.error("Unsupported field name: %s", field_name)
            raise Exception("Unsupported field name")


def _list_of_list_to_list_of_tuples(data: List[List[Any]]) -> List[tuple]:
    return [tuple(l) for l in data]


def _simplified_dtype(data_type):
    """
    Return the athena base data type
    Args:
        data_type (string): data type
    :return: simplified data type
    """
    return ((re.sub(r'\(.*\)', '', data_type)).split(" ", 1)[0]).upper()


def _get_typed_data_frame(data_frame: DataFrame, data_types: List[str],
                          field_names: List[str]) -> DataFrame:
    for data_type, field_name in zip(data_types, field_names):
        if data_type == 'NUMBER':
            data_frame[[field_name]] = data_frame[[field_name]].apply(pd.to_numeric)
        # TODO need to add date type support
    return data_frame


class SheetUtil:
    """
    Utility class for connecting to google sheets
    Args:
        conn_manager (GoogleSheetConnectionManager): connection to read google sheets
        conn_manager (integer): row number of the filed names
        conn_manager (integer): row number of the field types
    """

    def __init__(self, conn_manager: GoogleSheetConnectionManager, workbook_url, sheet):
        self.google_sheet_connection = conn_manager.get_client()
        self.workbook_url = workbook_url
        self.sheet = sheet
        self._worksheet = None

    def _get_worksheet(self):
        if self._worksheet is None:
            self._worksheet = self.google_sheet_connection.open_by_url(self.workbook_url).worksheet(
                self.sheet)
        return self._worksheet

    def get_value_matrix(self, row_range=None, data_start_row_number=None) -> List[List[Any]]:
        """
        Get the values of the sheet as a 2D array
        Args:
            row_range (string): [optional]range of the rows that need to be selected (eg: '1:7')
            data_start_row_number (integer): (eg: 2)
        Returns: values of the sheet as a 2D array
        """
        if not row_range and not data_start_row_number:
            ValueError(
                "Both row_range and data_start_row_number cannot be None, provide at least one")

        if row_range:
            return _get_value_matrix_from_range(worksheet=self._get_worksheet(),
                                                row_range=row_range)

        return _get_value_matrix_from_skip(worksheet=self._get_worksheet(),
                                           data_start_row_number=data_start_row_number)

    def get_fields_names(self, field_names_row_number: int) -> List[str]:
        """
        Get the field names as a list
        Args:
            field_names_row_number (int): row number of field names
        Returns: field names list
        """
        field_names = self._get_worksheet().row_values(field_names_row_number)
        LOG.debug("Field names:\n%s", field_names)
        return field_names

    def get_fields_types(self, field_types_row_number: int) -> List[str]:
        """
        Get the field types as a list
        Args:
            field_types_row_number (int): Row number of field types
        Returns: field types list
        """
        field_types = self._get_worksheet().row_values(field_types_row_number)
        LOG.debug("Field types:\n%s", field_types)
        return field_types

    def get_dataframe(self,
                      field_names_row_number: int,
                      field_types_row_number: int,
                      row_range=None,
                      data_start_row_number=None) -> DataFrame:
        """
        Get the field types as a list
        Args:
            field_names_row_number (int): Row number of field names
            field_types_row_number (int): Row number of field types
            row_range (str): Row range eg: "3:4"
            data_start_row_number (int): Starting row number of actual data
        Returns: field types list
        """
        matrix = self.get_value_matrix(row_range, data_start_row_number)
        return self._get_dataframe_from_matrix(field_names_row_number, field_types_row_number,
                                               matrix)

    def _get_dataframe_from_matrix(self, field_names_row_number, field_types_row_number, matrix):
        tupled_data = _list_of_list_to_list_of_tuples(matrix)
        field_names = self.get_fields_names(field_names_row_number)
        untyped_data_frame = DataFrame(data=tupled_data,
                                       columns=field_names)
        typed_data_frame = _get_typed_data_frame(data_frame=untyped_data_frame,
                                                 data_types=self.get_fields_types(
                                                     field_types_row_number),
                                                 field_names=field_names)
        return typed_data_frame
