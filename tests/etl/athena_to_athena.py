from unittest import TestCase
from unittest.mock import Mock

from hip_data_tools.etl.athena_to_athena import AthenaToAthena, AthenaToAthenaSettings


class TestAthenaToAthena(TestCase):
    def test_with_partitions(self):
        mock_settings = AthenaToAthenaSettings(
            source_sql="SELECT 'abc' as abc",
            source_database="test",
            target_database="test",
            target_table="test_table",
            target_data_format="PARQUET",
            target_s3_bucket="test_bucket",
            target_s3_dir="test_dir",
            target_partition_columns=["abc"],
            connection_settings=Mock()
        )
        etl = AthenaToAthena(mock_settings)
        actual = etl.generate_create_table_statement()
        expected = """
            CREATE TABLE test.test_table
            WITH (
                format = 'PARQUET'
                , external_location = 's3://test_bucket/test_dir/'
                , partitioned_by = ARRAY['abc'] 
            ) AS 
            SELECT 'abc' as abc
            """
        self.assertEqual(expected, actual)

    def test_without_partitions(self):
        mock_settings = AthenaToAthenaSettings(
            source_sql="SELECT 'abc' as abc",
            source_database="test",
            target_database="test",
            target_table="test_table",
            target_data_format="PARQUET",
            target_s3_bucket="test_bucket",
            target_s3_dir="test_dir",
            target_partition_columns=None,
            connection_settings=Mock()
        )
        etl = AthenaToAthena(mock_settings)
        actual = etl.generate_create_table_statement()
        expected = """
            CREATE TABLE test.test_table
            WITH (
                format = 'PARQUET'
                , external_location = 's3://test_bucket/test_dir/'
                
            ) AS 
            SELECT 'abc' as abc
            """
        self.assertEqual(expected, actual)
