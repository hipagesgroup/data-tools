import os
import textwrap
from unittest import TestCase
from unittest.mock import patch

from hip_data_tools.authenticate import AwsConnection
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.aws.spectrify import RedshiftSpectrifyUtil


class TestSpectrifyUtil(TestCase):

    @patch('hip_data_tools.aws.spectrify.RedshiftSpectrifyUtil')
    @patch('hip_data_tools.aws.redshift.RedshiftUtil')
    def test__prepare_unload_query(self, spectrifyUtil, redshiftUtil):
        redshiftUtil.schema = "test_schema"
        spectrifyUtil.ru = redshiftUtil
        spectrifyUtil.table_name = "test"
        athena_schema = [
            {'Field': "field_1", 'Type': "BIGINT"},
            {'Field': "field_2", 'Type': "BIGINT"},
            {'Field': "field_3", 'Type': "STRING"},
        ]

        actual = RedshiftSpectrifyUtil.prepare_unload_query(spectrifyUtil, athena_schema)

        self.assertEqual(actual, "select field_1, field_2, field_3 from test_schema.test")

    @patch('hip_data_tools.aws.spectrify.RedshiftSpectrifyUtil')
    @patch('hip_data_tools.aws.redshift.RedshiftUtil')
    def test__prepare_athena_table_ddl(self, spectrifyUtil, redshiftUtil):
        redshiftUtil.schema = "test_schema"
        os.environ["GANDALF_ENV"] = "dev"
        bucket = "test-bucket"
        conn = AwsConnection(mode="standard_env_var", region_name="ap-southeast-2", settings={})
        spectrifyUtil.s3u = S3Util(conn=conn, bucket=bucket)
        spectrifyUtil.s3_dir = "test_dir"
        spectrifyUtil.ru = redshiftUtil
        spectrifyUtil.table_name = "test"
        athena_schema = [
            {'Field': "field_1", 'Type': "BIGINT"},
            {'Field': "field_2", 'Type': "BIGINT"},
            {'Field': "field_3", 'Type': "STRING"},
        ]

        actual = RedshiftSpectrifyUtil.prepare_athena_table_ddl(spectrifyUtil, athena_schema)
        expected = """
        CREATE EXTERNAL TABLE IF NOT EXISTS `test` (
        field_1 BIGINT, field_2 BIGINT, field_3 STRING
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
        'separatorChar' = '|',
        'quoteChar' = '\\"',
        'escapeChar' = '\\\\'
        ) LOCATION 's3://test-bucket/test_dir/'
        TBLPROPERTIES ('has_encrypted_data'='false');
        """

        self.maxDiff = None
        # compare strings after removing whitespaces
        self.assertEqual(textwrap.dedent(actual).replace(" ", ""), textwrap.dedent(expected).replace(" ", ""))
