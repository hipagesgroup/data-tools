import json
from unittest import TestCase

from hip_data_tools.aws.common import AwsConnectionSettings

from hip_data_tools.etl.google_sheet_to_athena import GoogleSheetToAthena, \
    GoogleSheetsToAthenaSettings
from hip_data_tools.google.common import GoogleApiConnectionSettings


class TestS3Util(TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def integration_test_should__load_sheet_to_athena__when_using_sheetUtil(self):
        with open('../resources/key-file.json', 'r') as f:
            obj = json.load(f)
        print(obj)
        GoogleSheetToAthena(GoogleSheetsToAthenaSettings(
            source_workbook_url='https://docs.google.com/spreadsheets/d/1W1vICc6wQjumMr9QXNK0bneQCzKFsHacKyrzCBLfsQM/edit?usp=sharing',
            source_sheet='spec_example',
            source_row_range=None,
            source_fields=None,
            source_field_names_row_number=5,
            source_field_types_row_number=4,
            source_data_start_row_number=6,
            source_connection_settings=GoogleApiConnectionSettings(keys_object=obj),
            manual_partition_key_value={"column": "start_date", "value": "2020-03-10"},
            target_database='dev',
            target_table_name='test_sheets_example_v2',
            target_s3_bucket='au-com-hipages-data-scratchpad',
            target_s3_dir='sheets_example_v2',
            target_connection_settings=AwsConnectionSettings(region='ap-southeast-2',
                                                             profile='default',
                                                             secrets_manager=None),
            target_table_ddl_progress=True
        )).load_sheet_to_athena()


