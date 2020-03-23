"""
Module to deal with data transfer from Google sheets to S3
"""

from attr import dataclass

from hip_data_tools.aws.common import AwsConnectionManager
from hip_data_tools.aws.common import AwsConnectionSettings
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.google.common import GoogleApiConnectionSettings
from hip_data_tools.google.sheets import SheetUtil, GoogleSheetConnectionManager


@dataclass
class GoogleSheetsToS3Settings:
    """
    Google sheets to Athena ETL settings
    Args:
        source_workbook_url: str
        source_sheet: str
        source_row_range: str
        source_field_names_row_number: int
        source_field_types_row_number: int
        source_data_start_row_number: int
        source_connection_settings: GoogleApiConnectionSettings
        manual_partition_key_value: dict
        target_s3_bucket: str
        target_s3_dir: str
        target_connection_settings: AwsConnectionSettings
    """
    source_workbook_url: str
    source_sheet: str
    source_row_range: str
    source_field_names_row_number: int
    source_field_types_row_number: int
    source_data_start_row_number: int
    source_connection_settings: GoogleApiConnectionSettings
    manual_partition_key_value: dict
    target_s3_bucket: str
    target_s3_dir: str
    target_connection_settings: AwsConnectionSettings


class GoogleSheetToS3:
    """
    Class to transfer data from google sheet to athena
    Args:
        settings (GoogleSheetsToAthenaSettings): the settings around the etl to be executed
    """

    def __init__(self, settings: GoogleSheetsToS3Settings):
        self.__settings = settings
        self.data = None

    def _get_sheets_util(self):
        return SheetUtil(
            conn_manager=GoogleSheetConnectionManager(self.__settings.source_connection_settings),
            workbook_url=self.__settings.source_workbook_url,
            sheet=self.__settings.source_sheet)

    def _get_s3_util(self):
        return S3Util(
            bucket=self.__settings.target_s3_bucket,
            conn=AwsConnectionManager(settings=self.__settings.target_connection_settings))

    def _get_sheet_dataframe(self):
        if self.data is None:
            sheet_util = self._get_sheets_util()
            self.data = sheet_util.get_dataframe(
                field_names_row_number=self.__settings.source_field_names_row_number,
                field_types_row_number=self.__settings.source_field_types_row_number,
                row_range=self.__settings.source_row_range,
                data_start_row_number=self.__settings.source_data_start_row_number)
        return self.data

    def write_sheet_data_to_s3(self):
        """
        Write the data frame into S3
        Args:
            s3_key (string): s3 key
        :return: None
        """
        s3_util = self._get_s3_util()
        s3_util.upload_dataframe_as_parquet(
            dataframe=self._get_sheet_dataframe(),
            key=self.__settings.target_s3_dir,
            file_name="sheet_data")
