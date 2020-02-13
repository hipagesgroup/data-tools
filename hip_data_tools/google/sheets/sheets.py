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

    def get_table_settings(self, table_name, field_names, s3_bucket, s3_dir):
        """
        Get the table settings dictionary
        Args:
            table_name (string): name of the athena table
            field_names (list): list of field names
            s3_bucket (string): s3 bucket name
            s3_dir (string): s3 directory
        Returns: table settings dictionary

        """
        table_settings = {
            "table": table_name,
            "exists": True,
            "partitions": [],
            "columns": [],
            "storage_format_selector": "parquet",
            "s3_bucket": s3_bucket,
            "s3_dir": s3_dir,
            "encryption": False
        }
        columns = []
        for field_name in field_names:
            columns.append({"column": field_name, "type": "string"})
        table_settings["columns"] = columns

        return table_settings

    def get_the_insert_query(self, table_name, values_matrix):
        """
        Get the insert query for the athena table using the values matrix
        Args:
            table_name (string): name of the athena table
            values_matrix (array): values of the google sheet
        Returns: insert query for the athena table

        """
        if not values_matrix:
            return "INSERT INTO {table_name} VALUES ()".format(table_name=table_name)
        insert_query = "INSERT INTO {table_name} VALUES ".format(table_name=table_name)
        values = ""
        for value in values_matrix:
            values += "({}), ".format(', '.join(["'{}'".format(val) for val in value]))
        values = values[:-2]
        insert_query += values
        return insert_query
