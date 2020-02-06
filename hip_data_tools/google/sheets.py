import gspread

from hip_data_tools.aws.athena import AthenaUtil


class SheetUtil:

    def __init__(self, credentials, database, connection):
        self.gc = gspread.authorize(credentials)
        self.au = AthenaUtil(database=database, conn=connection)

    def _get_value_matrix(self, workbook_name, sheet_name, cell_range='', skip_top_rows_count=0):
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

    def _get_table_settings(self, table_name, field_names, s3_bucket, s3_dir):
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

    def _get_the_insert_query(self, table_name, values_matrix):
        if not values_matrix:
            return "INSERT INTO {table_name} VALUES ()".format(table_name=table_name)
        insert_query = "INSERT INTO {table_name} VALUES ".format(table_name=table_name)
        values = ""
        for value in values_matrix:
            values += "({}), ".format(', '.join(["'{}'".format(val) for val in value]))
        values = values[:-2]
        insert_query += values
        return insert_query

    def load_sheet_to_athena(self, workbook_name, sheet_name, table_name, field_names, s3_bucket, s3_dir,
                             skip_top_rows_count=0):
        """

        :param workbook_name:
        :param sheet_name:
        :param table_name:
        :param field_names:
        :param s3_bucket:
        :param s3_dir:
        :param skip_top_rows_count:
        :return:
        """
        values_matrix = self._get_value_matrix(workbook_name=workbook_name, sheet_name=sheet_name,
                                               skip_top_rows_count=skip_top_rows_count)
        table_settings = self._get_table_settings(table_name=table_name, field_names=field_names, s3_bucket=s3_bucket,
                                                  s3_dir=s3_dir)
        self.au.create_table(table_settings)
        insert_query = self._get_the_insert_query(table_name=table_name, values_matrix=values_matrix)
        self.au.run_query(query_string=insert_query)
