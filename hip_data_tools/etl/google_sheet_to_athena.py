"""
Module to deal with data transfer from Google sheets to Athena
"""
import re
from typing import List

from attr import dataclass

from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionManager
from hip_data_tools.aws.common import AwsConnectionSettings
from hip_data_tools.etl.google_sheet_to_s3 import GoogleSheetToS3
from hip_data_tools.google.common import GoogleApiConnectionSettings

DTYPE_GOOGLE_SHEET_TO_PARQUET_ATHENA = {
    "NUMBER": "DOUBLE",
    "STRING": "STRING",
    "BOOLEAN": "BOOLEAN",
    "DATE": "STRING"
}


@dataclass
class GoogleSheetsToAthenaSettings:
    """
    Google sheets to Athena ETL settings
    Args:
        workbook_name: the name of the workbook (eg: Tradie Acquisition Targets)
            If there are multiple workbooks with the same name, the workbook which has most recently
            shared with the google service account is used
        sheet_name: name of the google sheet (eg: sheet1)
        row_range: range of rows (eg: '2:5')
        table_name: name of the athena table (eg: 'sheet_table')
        --------make this a list of dictionary
        fields: list of sheet field names and types. Field names cannot contain hyphens('-'), spaces
            and special characters
            (eg: ['name:string','age:number','is_member:boolean'])
            If this is None, the field names and types from google sheet are used automatically
        field_names_row_number: row number of the field names (eg: 4). Will be ignored if fields
            have been specified (see above). Assumes the data starts at first column and there is no
            gaps. There should not be 2 fields with the same name.
        field_types_row_number: row number of the field types (eg: 5). Will be ignored if fields
            have been specified (see above)
        use_derived_types: if this is false type of the fields are considered as strings
            irrespective of the provided field types (eg: True)
        s3_bucket: s3 bucket to store the files (eg: au-test-bucket)
        s3_dir: s3 directory to store the files (eg: sheets/new)
        ---- call it manual_partition_key_value
        partition_key: list of partitions (eg: [{"column": "view", "type": "string"}]. Only one
            partition key can be used
        partition_value: value of the partition key (eg: '2020-02-14')
        ------- call it data_start_row_number --- it should be 6
        skip_top_rows_count: number of top rows that need to be skipped (eg: 1)
        ------ this should be GoogleSheetsConnectionSettings
        keys_object: google api keys dictionary object
            (eg: {'type': 'service_account', 'project_id': 'hip-gandalf-sheets',...... })
        ------ target_database
        database: name of the athena database (eg: dev)
        ------- target_connection_settings
        connection_settings: aws connection settings
    """

    source_workbook: str
    source_sheet: str
    source_row_range: str
    source_fields: list
    source_field_names_row_number: int
    source_field_types_row_number: int
    source_data_start_row_number: int
    source_connection_settings: GoogleApiConnectionSettings

    manual_partition_key_value: List[dict]
    target_database: str
    target_table_name: str
    target_s3_bucket: str
    target_s3_dir: str
    target_connection_settings: AwsConnectionSettings


def _simplified_dtype(data_type):
    """
    Return the athena base data type
    Args:
        data_type (string): data type
    :return: simplified data type
    """
    return ((re.sub(r'\(.*\)', '', data_type)).split(" ", 1)[0]).upper()


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
        self.write_sheet_data_to_s3()
        athena_util = self._get_athena_util()
        athena_util.create_table_from_dataframe_parquet(
            dataframe=self._get_sheet_dataframe(),
            s3_bucket=self.settings.target_s3_bucket,
            s3_dir=self.settings.target_s3_dir)
