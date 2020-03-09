import json
from unittest import TestCase

from hip_data_tools.aws.common import AwsConnectionSettings

from hip_data_tools.etl.google_sheet_to_athena import GoogleSheetToAthena, \
    GoogleSheetsToAthenaSettings
from hip_data_tools.google.common import GoogleApiConnectionSettings


class TestS3Util(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = GoogleSheetsToAthenaSettings(
            source_workbook_url='Test workbook',
            source_sheet='Sheet1',
            source_row_range='2:3',
            source_fields=['Jan_18:string', 'Feb_18:string', 'Mar_18:string', 'Apr_18:string',
                           'May_18:string',
                           'Jun_18:string', 'Jul_18:string', 'Aug_18:string', 'Sep_18:string',
                           'Oct_18:string',
                           'Nov_18:string', 'Dec_18:string', 'Jan_19:string', 'Feb_19:string',
                           'Mar_19:string',
                           'Apr_19:string', 'May_19:string', 'Jun_19:string', 'Jul_19:string',
                           'Aug_19:string',
                           'Sep_19:string', 'Oct_19:string', 'Nov_19:string', 'Dec_19:string',
                           'Jan_20:string',
                           'Feb_20:string', 'Mar_20:string', 'Apr_20:string', 'May_20:string',
                           'Jun_20:string'],
            source_field_names_row_number=4,
            source_field_types_row_number=5,
            source_data_start_row_number=6,
            source_connection_settings=GoogleApiConnectionSettings(keys_object=None),
            manual_partition_key_value={"column": "start_date", "value": "2020-03-08"},
            target_database='dev',
            target_table_name='test_sheets_example_v2',
            target_s3_bucket='au-com-hipages-data-scratchpad',
            target_s3_dir='sheets_example_v2',
            target_connection_settings=AwsConnectionSettings(region='ap-southeast-2',
                                                             profile='default',
                                                             secrets_manager=None),
            overwrite_target_table=False
        )
        cls.util = GoogleSheetToAthena(settings=cls.settings)

    @classmethod
    def tearDownClass(cls):
        return

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
            manual_partition_key_value=None,
            target_database='dev',
            target_table_name='test_sheets_example_v2',
            target_s3_bucket='au-com-hipages-data-scratchpad',
            target_s3_dir='sheets_example_v2',
            target_connection_settings=AwsConnectionSettings(region='ap-southeast-2',
                                                             profile='default',
                                                             secrets_manager=None),
            overwrite_target_table=True
        )).load_sheet_to_athena()

    def test_should__return_the_table_settings__when_using_sheetUtil(self):
        field_names = []
        field_types = []

        for field in self.settings.fields:
            field_name_type = field.split(':')
            field_names.append(field_name_type[0])
            field_types.append(field_name_type[1])

        actual = self.util._get_table_settings_for_sheets_table(field_names=field_names,
                                                                field_types=field_types)
        expected = {'columns': [{'column': 'Jan_18', 'type': 'STRING'},
                                {'column': 'Feb_18', 'type': 'STRING'},
                                {'column': 'Mar_18', 'type': 'STRING'},
                                {'column': 'Apr_18', 'type': 'STRING'},
                                {'column': 'May_18', 'type': 'STRING'},
                                {'column': 'Jun_18', 'type': 'STRING'},
                                {'column': 'Jul_18', 'type': 'STRING'},
                                {'column': 'Aug_18', 'type': 'STRING'},
                                {'column': 'Sep_18', 'type': 'STRING'},
                                {'column': 'Oct_18', 'type': 'STRING'},
                                {'column': 'Nov_18', 'type': 'STRING'},
                                {'column': 'Dec_18', 'type': 'STRING'},
                                {'column': 'Jan_19', 'type': 'STRING'},
                                {'column': 'Feb_19', 'type': 'STRING'},
                                {'column': 'Mar_19', 'type': 'STRING'},
                                {'column': 'Apr_19', 'type': 'STRING'},
                                {'column': 'May_19', 'type': 'STRING'},
                                {'column': 'Jun_19', 'type': 'STRING'},
                                {'column': 'Jul_19', 'type': 'STRING'},
                                {'column': 'Aug_19', 'type': 'STRING'},
                                {'column': 'Sep_19', 'type': 'STRING'},
                                {'column': 'Oct_19', 'type': 'STRING'},
                                {'column': 'Nov_19', 'type': 'STRING'},
                                {'column': 'Dec_19', 'type': 'STRING'},
                                {'column': 'Jan_20', 'type': 'STRING'},
                                {'column': 'Feb_20', 'type': 'STRING'},
                                {'column': 'Mar_20', 'type': 'STRING'},
                                {'column': 'Apr_20', 'type': 'STRING'},
                                {'column': 'May_20', 'type': 'STRING'},
                                {'column': 'Jun_20', 'type': 'STRING'}],
                    'encryption': False,
                    'exists': True,
                    'partitions': [{'column': 'start_date', 'type': 'string'}],
                    's3_bucket': 'au-com-hipages-data-scratchpad',
                    's3_dir': 'sheets',
                    'storage_format_selector': 'parquet',
                    'table': 'test_sheets'}
        self.assertEqual(expected, actual)

