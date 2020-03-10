"""
Module to deal with data transfer from Google sheets to Athena
"""

from attr import dataclass

from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionManager
from hip_data_tools.aws.common import AwsConnectionSettings
from hip_data_tools.etl.google_sheet_to_s3 import GoogleSheetToS3
from hip_data_tools.google.common import GoogleApiConnectionSettings


@dataclass
class GoogleSheetsToAthenaSettings:
    """
    Google sheets to Athena ETL settings
    Args:
        source_workbook_url: the url of the workbook
            (eg: https://docs.google.com/spreadsheets/d/1W1vIBLfsQM/edit?usp=sharing)
        source_sheet: name of the google sheet (eg: sheet1)
        source_row_range: range of rows (eg: '2:5')
        source_fields: list of dictionary sheet field names and types. Field names cannot contain
            hyphens('-'), spaces and special characters
            (eg: ['name:string','age:number','is_member:boolean'])
            If this is None, the field names and types from google sheet are used automatically
        source_field_names_row_number: row number of the field names (eg: 4). Will be ignored if
            fields have been specified (see above). Assumes the data starts at first column and
            there is no gaps. There should not be 2 fields with the same name.
        source_field_types_row_number: row number of the field types (eg: 5). Will be ignored if
            fields have been specified (see above)
        source_data_start_row_number: starting row number of the actual data
        source_connection_settings: GoogleApiConnectionSettings with google api keys dictionary
            object
        manual_partition_key_value: a dictionary with partition column name and value. Only one
            partition key can be used and this value need to be string
            (eg: {"column": "start_date", "value": "2020-03-08"})
        target_database: name of the athena database (eg: dev)
        target_table_name: name of the athena table (eg: 'sheet_table')
        target_s3_bucket: s3 bucket to store the files (eg: au-test-bucket)
        target_s3_dir: s3 directory to store the files (eg: sheets/new)
        target_connection_settings: aws connection settings
        target_table_ddl_progress: if this is true, the target table will be dropped and recreated
    """

    source_workbook_url: str
    source_sheet: str
    source_row_range: str
    source_fields: list
    source_field_names_row_number: int
    source_field_types_row_number: int
    source_data_start_row_number: int
    source_connection_settings: GoogleApiConnectionSettings
    manual_partition_key_value: dict
    target_database: str
    target_table_name: str
    target_s3_bucket: str
    target_s3_dir: str
    target_connection_settings: AwsConnectionSettings
    target_table_ddl_progress: bool


class GoogleSheetToAthena(GoogleSheetToS3):
    """
    Class to transfer data from google sheet to athena
    Args:
        settings (GoogleSheetsToAthenaSettings): the settings around the etl to be executed
    """

    def __init__(self, settings: GoogleSheetsToAthenaSettings):
        super().__init__(settings)
        self.settings = settings
        self.keys_to_transfer = None

    def _get_athena_util(self):
        return AthenaUtil(database=self.settings.target_database,
                          conn=AwsConnectionManager(
                              settings=self.settings.target_connection_settings),
                          output_bucket=self.settings.target_s3_bucket)

    def load_sheet_to_athena(self):
        s3_key_with_partition = self.settings.target_s3_dir
        if self.settings.manual_partition_key_value is not None:
            column_name = self.settings.manual_partition_key_value["column"]
            column_value = self.settings.manual_partition_key_value["value"]
            partition_path = f"/{column_name}={column_value}"
            s3_key_with_partition += partition_path
        self.write_sheet_data_to_s3(s3_key_with_partition)

        athena_util = self._get_athena_util()
        if self.settings.target_table_ddl_progress:
            athena_util.drop_table(self.settings.target_table_name)

        athena_util.create_table_from_dataframe_parquet(
            dataframe=self._get_sheet_dataframe(),
            partitions=self.settings.manual_partition_key_value,
            table=self.settings.target_table_name,
            s3_bucket=self.settings.target_s3_bucket,
            s3_dir=self.settings.target_s3_dir)
        athena_util.repair_table_partitions(table=self.settings.target_table_name)
