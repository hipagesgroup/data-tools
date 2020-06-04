from unittest import TestCase

import pandas as pd
from moto import mock_s3
from pandas.util.testing import assert_frame_equal

from hip_data_tools.aws.common import AwsConnectionSettings, AwsSecretsManager, AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.etl.common import S3DirectorySource
from hip_data_tools.etl.s3_to_dataframe import S3ToDataFrame


class TestS3ToDataFrame(TestCase):

    @mock_s3
    def test_the_etl_should_list_files_correctly(self):
        test_bucket = "TEST_BUCKET_DEST"
        test_key = "some/key"
        aws_conn = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        s3_util = S3Util(conn=AwsConnectionManager(aws_conn), bucket=test_bucket)
        df1 = pd.DataFrame(dict(A=range(10000)),
                           index=pd.date_range('20130101', periods=10000, freq='s'))
        df2 = pd.DataFrame(dict(A=range(10000)),
                           index=pd.date_range('20140101', periods=10000, freq='s'))
        df3 = pd.DataFrame(dict(A=range(10000)),
                           index=pd.date_range('20150101', periods=10000, freq='s'))

        s3_util.create_bucket()
        s3_util.upload_dataframe_as_parquet(df1, test_key, "df1")
        s3_util.upload_dataframe_as_parquet(df2, test_key, "df2")
        s3_util.upload_dataframe_as_parquet(df3, test_key, "df3")

        settings = S3DirectorySource(
            bucket=test_bucket,
            directory_key=test_key,
            connection_settings=aws_conn,
        )

        etl = S3ToDataFrame(settings)

        expected_s3_files = ['some/key/df1.parquet', 'some/key/df2.parquet',
                             'some/key/df3.parquet']
        self.assertEqual(expected_s3_files, etl.list_source_files())

    @mock_s3
    def test_the_etl_should_download_first_file_correctly(self):
        test_bucket = "SOME_OTHER_BUCKET"
        test_key = "some/key"
        aws_conn = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        s3_util = S3Util(conn=AwsConnectionManager(aws_conn), bucket=test_bucket)
        df1 = pd.DataFrame(dict(A=range(10)),
                           index=pd.date_range('20130101', periods=10, freq='d'))
        df2 = pd.DataFrame(dict(A=range(10, 20)),
                           index=pd.date_range('20130111', periods=10, freq='d'))

        s3_util.create_bucket()
        s3_util.upload_dataframe_as_parquet(df1, test_key, "df1")
        s3_util.upload_dataframe_as_parquet(df2, test_key, "df2")

        settings = S3DirectorySource(
            bucket=test_bucket,
            directory_key=test_key,
            connection_settings=aws_conn,
        )

        etl = S3ToDataFrame(settings)

        expected = pd.DataFrame(dict(A=range(20)),
                                index=pd.date_range('20130101', periods=20, freq='d'))
        assert_frame_equal(expected, etl.get_all_files_as_data_frame())

    @mock_s3
    def test_the_etl_should_list_files_correctly(self):
        test_bucket = "TEST_BUCKET_ITR"
        test_key = "some/key"
        aws_conn = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        s3_util = S3Util(conn=AwsConnectionManager(aws_conn), bucket=test_bucket)
        df1 = pd.DataFrame(dict(A=range(10000)),
                           index=pd.date_range('20130101', periods=10000, freq='s'))
        df2 = pd.DataFrame(dict(A=range(10000)),
                           index=pd.date_range('20140101', periods=10000, freq='s'))
        df3 = pd.DataFrame(dict(A=range(10000)),
                           index=pd.date_range('20150101', periods=10000, freq='s'))

        s3_util.create_bucket()
        s3_util.upload_dataframe_as_parquet(df1, test_key, "df1")
        s3_util.upload_dataframe_as_parquet(df2, test_key, "df2")
        s3_util.upload_dataframe_as_parquet(df3, test_key, "df3")

        settings = S3DirectorySource(
            bucket=test_bucket,
            directory_key=test_key,
            connection_settings=aws_conn,
        )

        etl = S3ToDataFrame(settings)

        assert_frame_equal(df1, etl.next())
        assert_frame_equal(df2, etl.next())
        assert_frame_equal(df3, etl.next())
