"""
Module to deal with data transfer from Google sheets to S3
"""

from hip_data_tools.aws.common import AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.etl.common import GoogleSheetsTableSource, S3DirectorySink
from hip_data_tools.google.sheets import SheetUtil, GoogleSheetConnectionManager


class GoogleSheetToDataFrame:
    """
    Class to transfer data from google sheet to DatFrame
    """

    def __init__(self, source: GoogleSheetsTableSource):
        self.__source = source
        self.data = None

    def _get_sheets_util(self):
        return SheetUtil(
            conn_manager=GoogleSheetConnectionManager(self.__source.connection_settings),
            workbook_url=self.__source.workbook_url,
            sheet=self.__source.sheet)

    def get_sheet_as_dataframe(self):
        if self.data is None:
            sheet_util = self._get_sheets_util()
            self.data = sheet_util.get_dataframe(
                field_names_row_number=self.__source.field_names_row_number,
                field_types_row_number=self.__source.field_types_row_number,
                row_range=self.__source.row_range,
                data_start_row_number=self.__source.data_start_row_number)
        return self.data


class GoogleSheetToS3(GoogleSheetToDataFrame):
    """
    Class to transfer data from google sheet to athena
    """

    def __init__(self, source: GoogleSheetsTableSource, sink: S3DirectorySink):
        self.__source = source
        self.__sink = sink
        self.data = None
        super().__init__(source=source)

    def _get_s3_util(self):
        return S3Util(
            bucket=self.__sink.bucket,
            conn=AwsConnectionManager(settings=self.__sink.connection_settings))

    def write_sheet_data_to_s3(self):
        """
        Write the data frame into S3
        Args:
            s3_key (string): s3 key
        :return: None
        """
        s3_util = self._get_s3_util()
        prefix = ""
        if self.__sink.file_prefix:
            prefix = self.__sink.file_prefix
        s3_util.upload_dataframe_as_parquet(
            dataframe=self.get_sheet_as_dataframe(),
            key=self.__sink.directory_key,
            file_name=f"{prefix}sheet_data")
