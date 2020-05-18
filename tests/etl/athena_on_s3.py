import os
from unittest import TestCase

from pandas import DataFrame
from py._builtin import execfile

from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionSettings, AwsSecretsManager, AwsConnectionManager
from hip_data_tools.etl.athena_on_s3 import AthenaOnS3Settings, AthenaOnS3
from hip_data_tools.etl.dataframe_to_s3 import DataFrameToS3, DataFrameToS3Settings


class TestS3ToS3(TestCase):

    def test__should_be_able_to_work_with_dataframe_without_partitions(self):
        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        execfile("../../secrets.py")
        target_bucket = os.getenv("S3_TEST_BUCKET")
        target_prefix = "target/prefix"
        target_table = "test_table_created_by_AthenaOnS3__1"
        DataFrameToS3(
            DataFrameToS3Settings(
                source_dataframe=DataFrame([
                    {"abc": "def", "num": 1.0},
                    {"abc": "pqr", "num": 1.0},
                    {"abc": "", "num": 0}, ]),
                target_bucket=target_bucket,
                target_key_prefix=target_prefix,
                target_file_name="data",
                target_connection_settings=aws_setting
            )
        ).upload()

        etl = AthenaOnS3(
            AthenaOnS3Settings(
                bucket=target_bucket,
                base_key_prefix=target_prefix,
                target_database=os.getenv("dummy_athena_database"),
                target_table=target_table,
                target_is_partitioned_table=False,
                partition_columns=None,
                target_connection_settings=aws_setting
            )
        )
        etl.create_athena_table()

        au = AthenaUtil(database=os.getenv("dummy_athena_database"),
                        conn=AwsConnectionManager(aws_setting))

        actual = au.get_table_columns(table=target_table)
        expected = ([{'Name': 'abc', 'Type': 'string'}, {'Name': 'num', 'Type': 'double'}], [])
        self.assertEqual(expected, actual)

    def test__should_be_able_to_work_with_partitions(self):
        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        execfile("../../secrets.py")
        target_bucket = os.getenv("S3_TEST_BUCKET")
        target_prefix = "target/prefix2"
        partitioned_prefix = f"{target_prefix}/part1=abc/part2=111"
        target_table = "test_table_created_by_AthenaOnS3__2"
        DataFrameToS3(
            DataFrameToS3Settings(
                source_dataframe=DataFrame([
                    {"abc": "def", "num": 1.0},
                    {"abc": "pqr", "num": 1.0},
                    {"abc": "", "num": 0},
                ]),
                target_bucket=target_bucket,
                target_key_prefix=partitioned_prefix,
                target_file_name="data",
                target_connection_settings=aws_setting
            )
        ).upload()

        etl = AthenaOnS3(
            AthenaOnS3Settings(
                bucket=target_bucket,
                base_key_prefix=target_prefix,
                target_database=os.getenv("dummy_athena_database"),
                target_table=target_table,
                target_is_partitioned_table=True,
                partition_columns=[("part1", "STRING"), ("part2", "INT")],
                target_connection_settings=aws_setting
            )
        )
        etl.create_athena_table()

        au = AthenaUtil(database=os.getenv("dummy_athena_database"),
                        conn=AwsConnectionManager(aws_setting))

        actual = au.get_table_columns(table=target_table)
        expected = ([{'Name': 'abc', 'Type': 'string'}, {'Name': 'num', 'Type': 'double'}],
                    [{'Name': 'part1', 'Type': 'string'}, {'Name': 'part2', 'Type': 'int'}])
        self.assertEqual(expected, actual)
