import os
import uuid
from unittest import TestCase

from moto import mock_s3

from hip_data_tools.authenticate import AwsConnection
from hip_data_tools.aws.s3 import S3Util


class TestS3Util(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sample_file_location = "./test_sample.txt"
        cls.sample_file_content = str(uuid.uuid4())
        with open(cls.sample_file_location, 'w+') as f:
            f.write(cls.sample_file_content)

    @classmethod
    def tearDownClass(cls):
        os.remove(cls.sample_file_location)

    @mock_s3
    def test_should__upload_then_download_file_from_s3__when_using_s3util(self):
        bucket = "TEST_BUCKET"
        conn = AwsConnection(mode="standard_env_var", region_name="ap-southeast-2", settings={})
        s3u = S3Util(conn=conn, bucket=bucket)
        s3u.create_bucket()
        upload_key = "temp.txt"
        s3u.upload_file(local_file_path=self.sample_file_location, s3_key=upload_key)
        redown_file = "{}re".format(self.sample_file_location)
        s3u.download_file(local_file_path=redown_file, s3_key=upload_key)
        with open(redown_file, 'r') as f:
            redown_content = f.read()

        self.assertEqual(self.sample_file_content, redown_content)
