from unittest import TestCase
from unittest.mock import Mock

from hip_data_tools.etl.athena_to_athena import AthenaToAthena, \
    AthenaCTASSink
from hip_data_tools.etl.common import AthenaQuerySource


class TestAthenaToAthena(TestCase):
    def test_with_partitions(self):
        source = AthenaQuerySource(
            sql="SELECT 'abc' as abc",
            database="test",
            query_result_bucket="TEST",
            query_result_key="TEST",
            connection_settings=Mock())
        sink = AthenaCTASSink(
            database="test",
            table="test_table",
            data_format="PARQUET",
            s3_data_location_bucket="test_bucket",
            s3_data_location_directory_key="test_dir",
            partition_columns=["abc"]
        )
        etl = AthenaToAthena(source=source, sink=sink)
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
        source = AthenaQuerySource(
            sql="SELECT 'abc' as abc",
            database="test",
            query_result_bucket="TEST",
            query_result_key="TEST",
            connection_settings=Mock())
        sink = AthenaCTASSink(
            database="test",
            table="test_table",
            data_format="PARQUET",
            s3_data_location_bucket="test_bucket",
            s3_data_location_directory_key="test_dir",
            partition_columns=None
        )
        etl = AthenaToAthena(source=source, sink=sink)
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
