from collections import OrderedDict
from unittest import TestCase
from unittest.mock import Mock

from pandas import DataFrame

import hip_data_tools.aws.athena as athena
from hip_data_tools.aws.athena import AthenaUtil


class TestAthenaUtil(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.au = AthenaUtil(database="test", conn=None)

    def test__build_create_table_sql__works_for_ga_example(self):
        actual = self.au._build_create_table_sql(
            table_settings={
                "table": "abc",
                "exists": True,
                "partitions": [
                    {"column": "view", "type": "string"},
                    {"column": "start_date_key", "type": "bigint"},
                    {"column": "end_date_key", "type": "bigint"},
                ],
                "columns": [
                    {"column": "appversion", "type": "string"},
                    {"column": "date", "type": "string"},
                    {"column": "goal1completions", "type": "string"},
                    {"column": "goal1starts", "type": "string"},
                    {"column": "mobiledeviceinfo", "type": "string"},
                ],
                "storage_format_selector": "parquet",
                "s3_bucket": "test",
                "s3_dir": "abc",
                "encryption": False
            }
        )
        expected = """
            CREATE EXTERNAL TABLE IF NOT EXISTS abc(
              appversion string, date string, goal1completions string, goal1starts string, 
              mobiledeviceinfo string
              )

            PARTITIONED BY (
              view string, start_date_key bigint, end_date_key bigint
              )

            ROW FORMAT SERDE
              'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            STORED AS INPUTFORMAT
              'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
            OUTPUTFORMAT
              'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            LOCATION
              's3://test/abc'
            TBLPROPERTIES ('has_encrypted_data'='false')
        """
        print(actual)
        self.maxDiff = None
        self.assertEqual(actual.split(), expected.split())

    def test__build_create_table_for_csv_data(self):
        actual = self.au._build_create_table_sql(
            table_settings={
                "table": "branch_reports",
                "exists": True,
                "partitions": [
                    {"column": "source", "type": "string"},
                    {"column": "report", "type": "string"},
                    {"column": "date_dim_key", "type": "string"},
                ],
                "columns": [
                    {"column": "id", "type": "string"},
                    {"column": "name", "type": "string"},
                    {"column": "timestamp", "type": "string"},
                    {"column": "timestamp_iso", "type": "string"},
                    {"column": "origin", "type": "string"},
                ],
                "storage_format_selector": "csv",
                "s3_bucket": "test",
                "s3_dir": "data/external/",
                "encryption": False,
                "skip_headers": True
            }
        )

        expected = """
            CREATE EXTERNAL TABLE IF NOT EXISTS branch_reports(
               id string, name string, timestamp string, timestamp_iso string, origin string
            )
            PARTITIONED BY (
                source string, report string, date_dim_key string
            )
            ROW FORMAT SERDE 
                'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            STORED AS INPUTFORMAT
                'org.apache.hadoop.mapred.TextInputFormat'
            OUTPUTFORMAT
                'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION
                's3://test/data/external/'
            TBLPROPERTIES ('has_encrypted_data'='false', 'skip.header.line.count'='1')
        """
        print(actual)
        self.maxDiff = None
        self.assertEqual(actual.split(), expected.split())

    def test__generate_parquet_ctas__creates_correct_syntax(self):
        actual = athena.generate_parquet_ctas(
            select_query="SELECT abc FROM def",
            destination_table="test",
            destination_bucket="test",
            destination_key="test")
        expected = """        
        CREATE TABLE test
        WITH (
            format='parquet',
            external_location='s3://test/test'
        ) AS
        SELECT abc FROM def
        """
        self.assertEqual(actual.split(), expected.split())

    def test__generate_csv_ctas__creates_correct_syntax(self):
        actual = athena.generate_csv_ctas(
            select_query="SELECT abc FROM def",
            destination_table="test",
            destination_bucket="test",
            destination_key="test")
        expected = """        
        CREATE TABLE test
        WITH (
            field_delimiter=',',
            format='TEXTFILE',
            external_location='s3://test/test'
        ) AS
        SELECT abc FROM def
        """
        self.assertEqual(actual.split(), expected.split())

    def test__zip_columns__works_with_one(self):
        input = [{
            "column": "abc",
            "type": "def"
        }]
        actual = athena.zip_columns(input)
        expected = "abc def"
        self.assertEqual(actual, expected)

    def test__zip_columns__works_with_two(self):
        input = [{
            "column": "abc",
            "type": "def"
        }, {
            "column": "pqr",
            "type": "stu"
        }]
        actual = athena.zip_columns(input)
        expected = "abc def, pqr stu"
        self.assertEqual(actual, expected)

    def test__zip_columns__works_with_none(self):
        input = []
        actual = athena.zip_columns(input)
        expected = ""
        self.assertEqual(actual, expected)

    def test_drop_table__works_as_intended(self):
        with self.assertRaises(AttributeError):
            self.au.drop_table("abc")

    def test__get_table_data_location__should_return_an_s3_location(self):
        mock_au = Mock()
        expected = ("abc", "def/pqr/")
        # 
        mock_au.get_glue_table_metadata.return_value = {
            'Table': {
                'StorageDescriptor': {
                    'Location': "s3://abc/def/pqr/",
                }
            }
        }
        actual = AthenaUtil.get_table_data_location(mock_au, "test")
        self.assertEqual(actual, expected)

    def test__get_table_columns__should_return_tuple_when_partition_is_present(self):
        mock_au = Mock()
        expected = ([
                        {"Name": "first_column", "Type": "string"},
                        {"Name": "second_column", "Type": "string"},
                    ], [
                        {"Name": "first_partition_column", "Type": "string"},
                        {"Name": "second_partition_column", "Type": "string"}
                    ])
        #
        mock_au.get_glue_table_metadata.return_value = {
            "Table":
                {
                    "Name":
                        "test",
                    "DatabaseName": "test",
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "first_column", "Type": "string"},
                            {"Name": "second_column", "Type": "string"},
                        ],
                    },
                    "PartitionKeys": [
                        {"Name": "first_partition_column", "Type": "string"},
                        {"Name": "second_partition_column", "Type": "string"}
                    ]
                }
        }

        actual = AthenaUtil.get_table_columns(mock_au, "test")
        self.assertEqual(actual, expected)

    def test__get_table_columns__should_return_tuple_when_partition_is_not_present(self):
        mock_au = Mock()
        expected = ([
                        {"Name": "first_column", "Type": "string"},
                        {"Name": "second_column", "Type": "string"},
                    ], [])
        #
        mock_au.get_glue_table_metadata.return_value = {
            "Table":
                {
                    "Name":
                        "test",
                    "DatabaseName": "test",
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "first_column", "Type": "string"},
                            {"Name": "second_column", "Type": "string"},
                        ],
                    },
                    "PartitionKeys": []
                }
        }

        actual = AthenaUtil.get_table_columns(mock_au, "test")
        self.assertEqual(actual, expected)

    def test__get_table_settings_for_sheets_table__should_return_table_settings(self):
        expected = {'exists': True, 'partitions': None, 'storage_format_selector': 'parquet',
                    'encryption': False, 'table': 'branch_reports',
                    'columns': [{'column': 'source', 'type': 'STRING'},
                                {'column': 'report', 'type': 'STRING'}], 's3_bucket': 'test',
                    's3_dir': 'data/external/'}

        actual = athena.get_table_settings_for_dataframe(dataframe=DataFrame(
            data=[{"source": "source_value_1", "report": "report_value_1"},
                  {"source": "source_value_2", "report": "report_value_2"}]), partitions=None,
            s3_bucket="test",
            s3_dir="data/external/", table="branch_reports")

        print(actual)
        self.assertEqual(actual, expected)

    def test__get_athena_columns_from_dataframe__should__return_col_names_and_types__when__a_data_frame_is_given(
            self):
        expected = [{'column': 'field_1', 'type': 'STRING'},
                    {'column': 'field_2', 'type': 'BIGINT'},
                    {'column': 'field_3', 'type': 'STRING'},
                    {'column': 'field_4', 'type': 'BOOLEAN'},
                    {'column': 'field_5', 'type': 'STRING'},
                    {'column': 'field_6', 'type': 'DOUBLE'}]
        actual = athena.get_athena_columns_from_dataframe(data_frame=DataFrame(
            data=[{"field_1": "sample str value", "field_2": 343, "field_3": None, "field_4": True,
                   "field_5": OrderedDict(
                       [('policyTopicEntries', []), ('reviewState', 'REVIEWED')]),
                   "field_6": 2.3434}]))
        self.assertEqual(actual, expected)
