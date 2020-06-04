import json
from unittest import TestCase

from past.builtins import execfile

from hip_data_tools.aws.common import AwsConnectionSettings
from hip_data_tools.common import DictKeyValueSource
from hip_data_tools.etl.common import GoogleSheetsTableSource, AthenaTableDirectorySink
from hip_data_tools.etl.google_sheet_to_athena import GoogleSheetToAthena
from hip_data_tools.google.common import GoogleApiConnectionSettings, GoogleApiSecretsManager


class TestS3Util(TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_should__load_sheet_to_athena__when_using_sheetUtil(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        with open('../resources/key-file.json', 'r') as f:
            obj = json.load(f)

        GoogleSheetToAthena(
            source=GoogleSheetsTableSource(
                workbook_url='https://docs.google.com/spreadsheets/d'
                             '/1W1vICFsHacKyrzCBLfsQM/edit?usp=sharing',
                sheet='spec_example',
                row_range=None,
                field_names_row_number=5,
                field_types_row_number=4,
                data_start_row_number=6,
                connection_settings=GoogleApiConnectionSettings(
                    secrets_manager=GoogleApiSecretsManager(
                        source=DictKeyValueSource({
                            "key_json": obj
                        }),
                        key_json_var="key_json"
                    )
                ),
            ),
            sink=AthenaTableDirectorySink(
                partition_value=[("column", "start_date"), ("value", "2020-03-11")],
                database='dev',
                table='test_sheets_example_v2',
                query_result_bucket=os.environ['S3_TEST_BUCKET'],
                query_result_key='sheets_example_v2',
                connection_settings=AwsConnectionSettings(
                    region='ap-southeast-2',
                    profile='default',
                    secrets_manager=None),
                table_ddl_progress=True
            )
        ).load_sheet_to_athena()
