from unittest import TestCase

from hip_data_tools.etl.google_sheet_to_athena import GoogleSheetToAthena, GoogleSheetsToAthenaSettings
from hip_data_tools.google.common import GoogleApiConnectionManager, GoogleApiConnectionSettings
from hip_data_tools.google.sheets import SheetUtil


class TestS3Util(TestCase):
    @classmethod
    def setUpClass(cls):
        credentials = GoogleApiConnectionManager(
            GoogleApiConnectionSettings(key_file_path='../resources/key-file.json')).credentials(service='sheet')
        cls.sheet_util = SheetUtil(credentials=credentials)

    @classmethod
    def tearDownClass(cls):
        return

    def test_should__load_sheet_to_athena__when_using_sheetUtil(self):
        self.skipTest("This is an integration test")
        util = GoogleSheetToAthena(GoogleSheetsToAthenaSettings(
            key_file_path='../resources/key-file.json',
            workbook_name='Tradie Acquisition Targets',
            sheet_name='Sheet1',
            table_name='test_sheets',
            field_names=['Jan_18', 'Feb_18', 'Mar_18', 'Apr_18', 'May_18', 'Jun_18', 'Jul_18', 'Aug_18', 'Sep_18',
                         'Oct_18',
                         'Nov_18', 'Dec_18', 'Jan_19', 'Feb_19', 'Mar_19', 'Apr_19', 'May_19', 'Jun_19', 'Jul_19',
                         'Aug_19',
                         'Sep_19', 'Oct_19', 'Nov_19', 'Dec_19', 'Jan_20', 'Feb_20', 'Mar_20', 'Apr_20', 'May_20',
                         'Jun_20'],
            database='dev',
            # TODO use different bucket
            s3_bucket='au-com-hipages-data-scratchpad',
            s3_dir='sheets',
            skip_top_rows_count=1,
            region='ap-southeast-2',
            profile='default',
            secrets_manager=None
        ))
        util.load_sheet_to_athena()
