import gspread


class SheetUtil:

    def __init__(self, credentials):
        self.gc = gspread.authorize(credentials)

    def get_all_values(self, workbook_name, sheet_name, cell_range='', skip_top_rows_count=0):
        """

        :param skip_top_rows_count:
        :param cell_range:
        :param sheet_name:
        :param workbook_name:
        :return:
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

    def get_table_settings(self, table_name, field_names, partitions, s3_bucket, s3_dir):
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

        partition_settings = []
        for partition_name in partitions:
            partition_settings.append({"column": partition_name, "type": "string"})
        table_settings["partitions"] = partition_settings

        return table_settings

    def get_the_update_query(self, values_matrix, field_names):
        return
