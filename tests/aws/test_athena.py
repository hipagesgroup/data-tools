from unittest import TestCase

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
              'org.apache.hadoop.mapred.TextInputFormat'
            OUTPUTFORMAT
              'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
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

    def test__build_create_table_for_parquet_data(self):
        actual = self.au._build_create_table_sql(
            table_settings={
                "table": "braze_consumer_segments",
                "exists": True,
                "partitions": [
                    {"column": "source", "type": "string"},
                    {"column": "report", "type": "string"},
                    {"column": "segment_id", "type": "string"},
                    {"column": "date_dim_key", "type": "string"}
                ],
                "columns": [
                    {"column": "appboy_id", "type": "string"},
                    {"column": "apps", "type": "string"},
                    {"column": "attributed_campaign", "type": "string"},
                    {"column": "attributed_source", "type": "string"},
                    {"column": "braze_id", "type": "string"},
                    {"column": "campaigns_received", "type": "string"},
                    {"column": "canvases_received", "type": "string"},
                    {"column": "cards_clicked", "type": "string"},
                    {"column": "country", "type": "string"},
                    {"column": "created_at", "type": "string"},
                    {"column": "custom_attributes", "type": "string"},
                    {"column": "custom_events", "type": "string"},
                    {"column": "devices", "type": "string"},
                    {"column": "email", "type": "string"},
                    {"column": "email_subscribe", "type": "string"},
                    {"column": "external_id", "type": "string"},
                    {"column": "first_name", "type": "string"},
                    {"column": "gender", "type": "string"},
                    {"column": "language", "type": "string"},
                    {"column": "last_name", "type": "string"},
                    {"column": "push_opted_in_at", "type": "string"},
                    {"column": "push_subscribe", "type": "string"},
                    {"column": "push_tokens", "type": "string"},
                    {"column": "random_bucket", "type": "string"},
                    {"column": "time_zone", "type": "string"},
                    {"column": "total_revenue", "type": "string"},
                    {"column": "uninstalled_at", "type": "string"},
                    {"column": "user_aliases", "type": "string"}
                ],
                "storage_format_selector": "parquet",
                "s3_bucket": "hipages-long-lake",
                "s3_dir": "data/external/",
                "encryption": False
            }
        )

        expected = """
           CREATE EXTERNAL TABLE IF NOT EXISTS braze_consumer_segments(
              appboy_id string, apps string, attributed_campaign string, attributed_source string, braze_id string, campaigns_received string, canvases_received string, cards_clicked string, country string, created_at string, custom_attributes string, custom_events string, devices string, email string, email_subscribe string, external_id string, first_name string, gender string, language string, last_name string, push_opted_in_at string, push_subscribe string, push_tokens string, random_bucket string, time_zone string, total_revenue string, uninstalled_at string, user_aliases string
              )
            
            PARTITIONED BY ( 
              source string, report string, segment_id string, date_dim_key string
              )
              
            ROW FORMAT SERDE 
              'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
            STORED AS INPUTFORMAT 
              'org.apache.hadoop.mapred.TextInputFormat' 
            OUTPUTFORMAT 
              'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION
              's3://hipages-long-lake/data/external/'
            
                TBLPROPERTIES ('has_encrypted_data'='false')
        """
        print(actual)
        self.maxDiff = None
        self.assertEquals(actual.split(), expected.split())

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
