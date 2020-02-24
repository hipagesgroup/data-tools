import json
from unittest import TestCase

from hip_data_tools.aws.common import AwsConnectionSettings

from hip_data_tools.etl.google_sheet_to_athena import GoogleSheetToAthena, \
    GoogleSheetsToAthenaSettings


class TestS3Util(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.util = GoogleSheetToAthena(GoogleSheetsToAthenaSettings(
            keys_object=None,
            workbook_name='Tradie Acquisition Targets',
            sheet_name='Sheet1',
            row_range='2:3',
            table_name='test_sheets',
            fields=['Jan_18:string', 'Feb_18:string', 'Mar_18:string', 'Apr_18:string',
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
            use_derived_types=True,
            database='dev',
            # TODO use different bucket
            s3_bucket='au-com-hipages-data-scratchpad',
            s3_dir='sheets',
            partition_key=[{"column": "start_date", "type": "string"}],
            partition_value='2020-02-14',
            skip_top_rows_count=1,
            connection_settings=AwsConnectionSettings(region='ap-southeast-2',
                                                      profile='default',
                                                      secrets_manager=None)
        ))

    @classmethod
    def tearDownClass(cls):
        return

    def integration_test_should__load_sheet_to_athena__when_using_sheetUtil(self):
        with open('../resources/key-file.json', 'r') as f:
            obj = json.load(f)
        print(obj)
        GoogleSheetToAthena(GoogleSheetsToAthenaSettings(
            keys_object=obj,
            workbook_name='Tradie Acquisition Targets',
            sheet_name='Sheet1',
            row_range='2:3',
            table_name='test_sheets',
            fields=['Jan_18:string', 'Feb_18:string', 'Mar_18:string', 'Apr_18:string',
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
            use_derived_types=True,
            database='dev',
            # TODO use different bucket
            s3_bucket='au-com-hipages-data-scratchpad',
            s3_dir='sheets',
            partition_key=[{"column": "start_date", "type": "string"}],
            partition_value='2020-02-14',
            skip_top_rows_count=1,
            connection_settings=AwsConnectionSettings(region='ap-southeast-2',
                                                      profile='default',
                                                      secrets_manager=None)
        )).load_sheet_to_athena()

    def test_should__return_the_table_settings__when_using_sheetUtil(self):
        actual = self.util._get_table_settings()
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

    def test_should__return_the_insert_query__when_using_sheetUtil(self):
        values_matix = [
            ['4,092', '3,192', '3,192', '2,800', '3,015', '3,015', '3,100', '3,415', '3,600',
             '3,570', '3,210',
             '1,900', '3,100', '2,747', '2,631', '2,419', '2,769', '3,163', '2,792', '3,018',
             '2,920', '3,541',
             '3,128', '2,020', '3,678', '3,522', '3,534', '3,078', '3,114', '3,206'],
            ['6,343', '4,192', '9,192', '2,800', '3,015', '3,015', '3,100', '3,415', '3,600',
             '3,570', '3,210',
             '1,900', '3,100', '2,747', '2,631', '2,419', '2,769', '3,163', '2,792', '3,018',
             '2,920', '3,541',
             '3,128', '2,020', '3,678', '3,522', '3,534', '3,078', '3,114', '8,206']]
        expected = """
                INSERT INTO test_sheets VALUES ('4,092', '3,192', '3,192', '2,800', '3,015', '3,015', '3,100', '3,415', '3,600', '3,570', '3,210', '1,900', '3,100', '2,747', '2,631', '2,419', '2,769', '3,163', '2,792', '3,018', '2,920', '3,541', '3,128', '2,020', '3,678', '3,522', '3,534', '3,078', '3,114', '3,206', '2020-02-14'), ('6,343', '4,192', '9,192', '2,800', '3,015', '3,015', '3,100', '3,415', '3,600', '3,570', '3,210', '1,900', '3,100', '2,747', '2,631', '2,419', '2,769', '3,163', '2,792', '3,018', '2,920', '3,541', '3,128', '2,020', '3,678', '3,522', '3,534', '3,078', '3,114', '8,206', '2020-02-14')            
                """
        actual = self.util._get_the_insert_query(values_matix)
        self.assertEqual(expected.strip(), actual.strip())
