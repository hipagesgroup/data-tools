from unittest import TestCase

from hip_data_tools.aws.common import AwsConnectionSettings
from hip_data_tools.etl.athena_to_dataframe import AthenaToDataFrameSettings, AthenaToDataFrame


class TestS3ToDataFrame(TestCase):

    def test_the_etl_should_work(self):
        test_database = "xxx"
        test_table = "xxx"
        aws_conn = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=None,
            profile="default")

        settings = AthenaToDataFrameSettings(
            source_database=test_database,
            source_table=test_table,
            source_connection_settings=aws_conn
        )

        etl = AthenaToDataFrame(settings)

        expected_s3_files = ['xxx']
        self.assertEqual(expected_s3_files, etl.list_source_files())

        self.assertEqual((47848, 28), etl.next().shape)

        self.assertEqual((47848, 28), etl.get_all_files_as_data_frame())
