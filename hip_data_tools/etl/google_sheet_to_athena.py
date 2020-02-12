"""
Module to deal with data transfer from Google sheets to Athena
"""
import logging as log

from attr import dataclass

from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionManager, AwsSecretsManager
from hip_data_tools.aws.common import AwsConnectionSettings
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.google.common import GoogleApiConnectionManager, GoogleApiConnectionSettings
from hip_data_tools.google.sheets import SheetUtil


@dataclass
class GoogleSheetsToAthenaSettings:
    """Google sheets to Athena ETL settings"""
    workbook_name: str
    sheet_name: str
    table_name: str
    field_names: list
    s3_bucket: str
    s3_dir: str
    skip_top_rows_count: int
    key_file_path: str
    database: str
    region: str
    profile: str
    secrets_manager: AwsSecretsManager


class GoogleSheetToAthena:
    """
    Class to transfer data from google sheet to athena
    Args:
        settings (GoogleSheetsToAthenaSettings): the settings around the etl to be executed
    """

    def __init__(self, settings: GoogleSheetsToAthenaSettings):
        self.settings = settings
        self.keys_to_transfer = None

    def _get_sheets_util(self):
        return SheetUtil(credentials=GoogleApiConnectionManager(
            GoogleApiConnectionSettings(key_file_path=self.settings.key_file_path)).credentials(service='sheet'))

    def _get_athena_util(self):
        return AthenaUtil(database=self.settings.database, conn=AwsConnectionManager(
            AwsConnectionSettings(region=self.settings.region, secrets_manager=self.settings.secrets_manager,
                                  profile=self.settings.profile)), output_bucket=self.settings.s3_bucket)

    def _get_s3_util(self):
        return S3Util(
            bucket=self.settings.s3_bucket, conn=AwsConnectionManager(
                AwsConnectionSettings(region=self.settings.region, secrets_manager=self.settings.secrets_manager,
                                      profile=self.settings.profile)))

    def load_sheet_to_athena(self, overwrite_table=False):
        """
        Method to load google sheet to athena
        :return: None
        """
        sheet_util = self._get_sheets_util()
        athena_util = self._get_athena_util()
        s3_util = self._get_s3_util()
        if overwrite_table:
            athena_util.drop_table(self.settings.table_name)
            s3_util.delete_recursive(self.settings.s3_dir)
        values_matrix = sheet_util.get_value_matrix(workbook_name=self.settings.workbook_name,
                                                    sheet_name=self.settings.sheet_name,
                                                    skip_top_rows_count=self.settings.skip_top_rows_count)
        log.info("The value matrix:\n %s", values_matrix)
        table_settings = sheet_util.get_table_settings(table_name=self.settings.table_name,
                                                       field_names=self.settings.field_names,
                                                       s3_bucket=self.settings.s3_bucket,
                                                       s3_dir=self.settings.s3_dir)
        athena_util.create_table(table_settings)
        insert_query = sheet_util.get_the_insert_query(table_name=self.settings.table_name, values_matrix=values_matrix)
        log.info("The insert query:\n %s", insert_query)
        athena_util.run_query(query_string=insert_query)
