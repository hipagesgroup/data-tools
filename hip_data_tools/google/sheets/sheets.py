"""
Module contains variables and methods used for google sheet operations
"""
from hip_data_tools.google.sheets.common import GoogleSheetConnectionManager


class SheetUtil:
    """
    Utility class for connecting to google sheets
    Args:
        conn_manager (GoogleSheetConnectionManager): connection to read google sheets
    """

    def __init__(self, conn_manager: GoogleSheetConnectionManager):
        self.google_sheet_connection = conn_manager.get_connection()

    def get_value_matrix(self, workbook_name, sheet_name, row_range='', skip_top_rows_count=0):
        """
        Get the values of the sheet as a 2D array
        Args:
            workbook_name (string): name of the workbook (eg: Tradie Acquisition Targets)
            sheet_name (string): name of the sheet (eg: sheet1)
            row_range (string): [optional]range of the rows that need to be selected (eg: '1:7')
            skip_top_rows_count (integer): (eg: 2)
        Returns: values of the sheet as a 2D array
        """
        worksheet = self.google_sheet_connection.open(workbook_name).worksheet(sheet_name)
        list_of_lists = []

        if not row_range:
            list_of_lists = worksheet.get_all_values()
            del list_of_lists[0:skip_top_rows_count]
            return list_of_lists

        row_numbers = [int(i) for i in row_range.split(':')]
        first_row_number = row_numbers[0]
        last_row_number = row_numbers[1]
        for i in range(first_row_number, last_row_number + 1):
            row = worksheet.row_values(i)
            list_of_lists.append(row)
        return list_of_lists
