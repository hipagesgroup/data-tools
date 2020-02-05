from unittest import TestCase

from oauth2client.service_account import ServiceAccountCredentials
from hip_data_tools.google.sheets import SheetUtil


class TestS3Util(TestCase):
    @classmethod
    def setUpClass(cls):
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            '../resources/hip-gandalf-sheets-5c4f5483f529.json', scope)
        cls.sheet_util = SheetUtil(credentials=credentials)

    @classmethod
    def tearDownClass(cls):
        return

    def test_should__return_the_values_in_a_given_google_sheet__when_using_sheetUtil(self):
        workbook_name = 'Tradie Acquisition Targets'
        sheet_name = 'Sheet1'
        actual = self.sheet_util.get_all_values(workbook_name, sheet_name)
        expected = [['Jan-18', 'Feb-18', 'Mar-18', 'Apr-18', 'May-18', 'Jun-18', 'Jul-18', 'Aug-18', 'Sep-18', 'Oct-18',
                     'Nov-18', 'Dec-18', 'Jan-19', 'Feb-19', 'Mar-19', 'Apr-19', 'May-19', 'Jun-19', 'Jul-19', 'Aug-19',
                     'Sep-19', 'Oct-19', 'Nov-19', 'Dec-19', 'Jan-20', 'Feb-20', 'Mar-20', 'Apr-20', 'May-20',
                     'Jun-20'],
                    ['4,092', '3,192', '3,192', '2,800', '3,015', '3,015', '3,100', '3,415', '3,600', '3,570', '3,210',
                     '1,900', '3,100', '2,747', '2,631', '2,419', '2,769', '3,163', '2,792', '3,018', '2,920', '3,541',
                     '3,128', '2,020', '3,678', '3,522', '3,534', '3,078', '3,114', '3,206']]
        print(actual)
        self.assertEqual(actual, expected)

    def test_should__return_the_table_settings__when_using_sheetUtil(self):
        table_name = 'abc'
        field_names = ['Jan-18', 'Feb-18', 'Mar-18', 'Apr-18', 'May-18', 'Jun-18', 'Jul-18', 'Aug-18', 'Sep-18',
                       'Oct-18',
                       'Nov-18', 'Dec-18', 'Jan-19', 'Feb-19', 'Mar-19', 'Apr-19', 'May-19', 'Jun-19', 'Jul-19',
                       'Aug-19',
                       'Sep-19', 'Oct-19', 'Nov-19', 'Dec-19', 'Jan-20', 'Feb-20', 'Mar-20', 'Apr-20', 'May-20',
                       'Jun-20']
        partitions = ['sheet']
        s3_bucket = "test"
        s3_dir = "abc"
        actual = self.sheet_util.get_table_settings(table_name, field_names, partitions, s3_bucket, s3_dir)
        expected = {
            "table": "abc",
            "exists": True,
            "partitions": [
                {"column": "sheet", "type": "string"}
            ],
            "columns": [
                {'column': 'Jan-18', 'type': 'string'},
                {'column': 'Feb-18', 'type': 'string'},
                {'column': 'Mar-18', 'type': 'string'},
                {'column': 'Apr-18', 'type': 'string'},
                {'column': 'May-18', 'type': 'string'},
                {'column': 'Jun-18', 'type': 'string'},
                {'column': 'Jul-18', 'type': 'string'},
                {'column': 'Aug-18', 'type': 'string'},
                {'column': 'Sep-18', 'type': 'string'},
                {'column': 'Oct-18', 'type': 'string'},
                {'column': 'Nov-18', 'type': 'string'},
                {'column': 'Dec-18', 'type': 'string'},
                {'column': 'Jan-19', 'type': 'string'},
                {'column': 'Feb-19', 'type': 'string'},
                {'column': 'Mar-19', 'type': 'string'},
                {'column': 'Apr-19', 'type': 'string'},
                {'column': 'May-19', 'type': 'string'},
                {'column': 'Jun-19', 'type': 'string'},
                {'column': 'Jul-19', 'type': 'string'},
                {'column': 'Aug-19', 'type': 'string'},
                {'column': 'Sep-19', 'type': 'string'},
                {'column': 'Oct-19', 'type': 'string'},
                {'column': 'Nov-19', 'type': 'string'},
                {'column': 'Dec-19', 'type': 'string'},
                {'column': 'Jan-20', 'type': 'string'},
                {'column': 'Feb-20', 'type': 'string'},
                {'column': 'Mar-20', 'type': 'string'},
                {'column': 'Apr-20', 'type': 'string'},
                {'column': 'May-20', 'type': 'string'},
                {'column': 'Jun-20', 'type': 'string'}
            ],
            "storage_format_selector": "parquet",
            "s3_bucket": "test",
            "s3_dir": "abc",
            "encryption": False
        }
        print(actual)
        self.assertEqual(actual, expected)
