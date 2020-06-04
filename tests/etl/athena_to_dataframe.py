import os
from unittest import TestCase

from past.builtins import execfile

from hip_data_tools.aws.common import AwsConnectionSettings
from hip_data_tools.etl.athena_to_dataframe import AthenaToDataFrame
from hip_data_tools.etl.common import AthenaTableSource


class TestS3ToDataFrame(TestCase):

    def test_the_etl_should_work(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        test_database = os.environ["dummy_athena_database"]
        test_table = os.environ["dummy_athena_table"]
        aws_conn = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=None,
            profile="default")

        source = AthenaTableSource(
            database=test_database,
            query_result_bucket=os.environ["dummy_athena_database"],
            query_result_key="TEST_RESULT_KEY",
            connection_settings=aws_conn,
            table=test_table,
        )

        etl = AthenaToDataFrame(source=source)

        expected_s3_files = []
        self.assertEqual(expected_s3_files, etl.list_source_files())

        # self.assertEqual((47848, 28), etl.next().shape)
        #
        # self.assertEqual((47848, 28), etl.get_all_files_as_data_frame())
