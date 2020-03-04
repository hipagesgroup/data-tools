"""
Module to handle connection and manipulation of google sheets
"""
import logging as log
import re

import gspread

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


class SheetUtil:
    """
    Utility class for connecting to google sheets
    Args:
        conn_manager (GoogleSheetConnectionManager): connection to read google sheets
        conn_manager (integer): row number of the filed names
        conn_manager (integer): row number of the field types
    """

    def __init__(self, conn_manager: GoogleSheetConnectionManager, workbook, sheet):
        self.google_sheet_connection = conn_manager.get_client()
        self.workbook = workbook
        self.sheet = sheet

    def get_value_matrix(self, row_range=None, data_start_row_number=None):
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
        worksheet = self.google_sheet_connection.open(self.workbook).worksheet(self.sheet)

        if row_range:
            return _get_value_matrix_from_range(worksheet, row_range)

        return _get_value_matrix_from_skip(worksheet, data_start_row_number)

    def get_fields_names(self, workbook_name, sheet_name, field_names_row_number: int):
        """
        Get the field names as a list
        Args:
            workbook_name (string): name of the workbook (eg: Revenue Targets)
            sheet_name (string): name of the sheet (eg: sheet1)
            field_names_row_number (int):
        Returns: field names list
        """
        worksheet = self.google_sheet_connection.open(workbook_name).worksheet(sheet_name)
        field_names = worksheet.row_values(field_names_row_number)
        log.debug("Field names:\n%s", field_names)
        return field_names

    def get_fields_types(self, workbook_name, sheet_name, field_types_row_number: int):
        """
        Get the field types as a list
        Args:
            workbook_name (string): name of the workbook (eg: Revenue Targets)
            sheet_name (string): name of the sheet (eg: sheet1)
            field_types_row_number (int):
        Returns: field types list
        """
        worksheet = self.google_sheet_connection.open(workbook_name).worksheet(sheet_name)
        field_types = worksheet.row_values(field_types_row_number)
        log.debug("Field types:\n%s", field_types)
        return field_types

    def get_dataframe(self, row_range=None, data_start_row_number=None):
        # TODO: Implement this
        matrix = self.get_value_matrix(row_range, data_start_row_number)
        pass

    def get_dict(self, row_range=None, data_start_row_number=None):
        # TODO: Implement this
        matrix = self.get_value_matrix(row_range, data_start_row_number)
        pass

    @staticmethod
    def __validate_field_names_and_types_count(field_names, field_types):
        if len(field_names) != len(field_types):
            log.error("Number of field names and number of field types are not matching")
            raise Exception("Field names and types are not matching")

    @staticmethod
    def __validate_field_names(field_names):
        for field_name in field_names:
            # check for strings which only contains letters, numbers and underscores
            if not re.match("^[A-Za-z0-9_]+$", field_name):
                log.error("Unsupported field name: %s", field_name)
                raise Exception("Unsupported field name")
