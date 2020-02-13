from hip_data_tools.google.sheets.common import GoogleSheetConnectionManager


class SheetUtil:
    """
    Utility class for connecting to google sheets
    Args:
        conn_manager (GoogleSheetConnectionManager): connection to read google sheets
    """

    def __init__(self, conn_manager: GoogleSheetConnectionManager):
        self.gc = conn_manager.get_connection()

    def get_value_matrix(self, workbook_name, sheet_name, cell_range='', skip_top_rows_count=0):
        """
        Get the values of the sheet as a 2D array
        Args:
            workbook_name (string): name of the workbook (eg: Tradie Acquisition Targets)
            sheet_name (string): name of the sheet (eg: sheet1)
            cell_range (string): range of the cells that need to be selected (eg: 'A1:B7') - this is optional
            skip_top_rows_count (integer): (eg: 2)
        Returns: values of the sheet as a 2D array

        """
        worksheet = self.gc.open(workbook_name).worksheet(sheet_name)
        list_of_lists = []

        if not cell_range:
            list_of_lists = worksheet.get_all_values()
            del list_of_lists[0:skip_top_rows_count]
            return list_of_lists
        else:
            for cell in worksheet.range(cell_range):
                list_of_lists.append(cell.value)
            return list_of_lists

