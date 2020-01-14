import os
import uuid
from unittest import TestCase
import json

import pandas as pd
from botocore.exceptions import ClientError
from moto import mock_s3
from pandas.util.testing import assert_frame_equal

from hip_data_tools.authenticate import AwsConnection
from hip_data_tools.aws.s3 import S3Util


class TestS3Util(TestCase):
    @classmethod
    def setUpClass(cls):
        dirname = "./test_folder_1"
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        cls.sample_dir_location = dirname
        cls.sample_file_location = dirname + "/test_sample.txt"
        cls.sample_json_location = dirname + "/test_json_sample.json"
        cls.sample_file_content = str(uuid.uuid4())
        cls.sample_json_content = "{}"
        with open(cls.sample_file_location, 'w+') as f:
            f.write(cls.sample_file_content)
        with open(cls.sample_json_location, 'w+') as f:
            f.write(cls.sample_json_location)

    @classmethod
    def tearDownClass(cls):
        os.remove(cls.sample_file_location)
        os.remove(cls.sample_json_location)
        os.remove(cls.sample_file_location + "re")
        os.remove(cls.sample_json_location + "re")

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

    @mock_s3
    def test_should__upload_then_download_directory_from_s3__when_using_s3util(self):
        bucket = "TEST_BUCKET"
        conn = AwsConnection(mode="standard_env_var", region_name="ap-southeast-2", settings={})
        s3u = S3Util(conn=conn, bucket=bucket)
        s3u.create_bucket()
        s3u.upload_file(local_file_path=self.sample_file_location, s3_key="test/test_sample_for_download_dir.txt")
        dirname = "./test_folder_2"
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        redown_file = dirname + "/test_sample_for_download_dir.txt"
        s3u.download_directory(source_key="test", file_suffix=".txt", local_directory=dirname)
        with open(redown_file, 'r') as f:
            redown_content = f.read()

        self.assertEqual(self.sample_file_content, redown_content)

    @mock_s3
    def test_should__serialise_deserialise_file_to_from_s3__when_using_s3util(self):
        bucket = "TEST_BUCKET2"
        conn = AwsConnection(mode="standard_env_var", region_name="ap-southeast-2", settings={})
        s3u = S3Util(conn=conn, bucket=bucket)
        s3u.create_bucket()
        upload_key = "temp.pickle"
        test_object = {"this": "is good"}
        s3u.serialise_and_upload_object(obj=test_object, s3_key=upload_key)
        actual_object = s3u.download_object_and_deserialse(s3_key=upload_key)
        self.assertEqual(test_object, actual_object)

    @mock_s3
    def test_should__upload_dataframe_and_download_parquet__when_using_s3util(self):
        bucket = "TEST_BUCKET3"
        conn = AwsConnection(mode="standard_env_var", region_name="ap-southeast-2", settings={})
        s3u = S3Util(conn=conn, bucket=bucket)
        s3u.create_bucket()
        upload_key = "temp.pickle"
        test_object = pd.DataFrame([1, 2, 3, 4], columns=["one"])
        s3u.upload_df_parquet(df=test_object, s3_key=upload_key)
        redown_df = s3u.download_df_parquet(upload_key)
        assert_frame_equal(test_object, redown_df)

    @mock_s3
    def test_should__copy_file_from_one_bucket_to_another__when_valid_locations_are_given(self):
        conn = AwsConnection(mode="standard_env_var", region_name="ap-southeast-2", settings={})
        source_bucket_name = "hipages-gandalf"
        dest_bucket_name = "au-com-hipages-data-scratchpad"

        s3_util_for_source = S3Util(conn=conn, bucket=source_bucket_name)
        s3_util_for_destination = S3Util(conn=conn, bucket=dest_bucket_name)

        s3_util_for_source.create_bucket()
        s3_util_for_destination.create_bucket()

        tmp_file_path = "/tmp/testfile.txt"
        dirname = os.path.dirname(tmp_file_path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        with open(tmp_file_path, "w+") as file:
            file.write(str("Test file content"))

        s3_util_for_source.upload_file(tmp_file_path, "test/testfile.txt")

        s3_util_for_source.move_recursive_to_different_bucket(source_dir="test/",
                                                              destination_bucket_name=dest_bucket_name,
                                                              destination_dir=dest_bucket_name + "/test_copy/")
        actual = s3_util_for_destination.read_lines_as_list("test_copy")[0]

        expected = "Test file content"
        self.assertEqual(actual, expected)

    @mock_s3
    def test_should__delete_recursive__when_using_s3util(self):
        bucket = "TEST_BUCKET"
        conn = AwsConnection(mode="standard_env_var", region_name="ap-southeast-2", settings={})
        s3u = S3Util(conn=conn, bucket=bucket)
        s3u.create_bucket()
        s3u.upload_file(local_file_path=self.sample_file_location, s3_key="test/test_delete_recursive.txt")
        s3u.delete_recursive("test")
        dirname = "./test_folder_3"
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        with self.assertRaises(ClientError):
            s3u.download_file(local_file_path=dirname + "/test_delete_recursive.txt",
                              s3_key="test/test_delete_recursive.txt")

    @mock_s3
    def test_should__delete_suffix__when_using_s3util(self):
        bucket = "TEST_BUCKET"
        conn = AwsConnection(mode="standard_env_var", region_name="ap-southeast-2", settings={})
        s3u = S3Util(conn=conn, bucket=bucket)
        s3u.create_bucket()
        s3u.upload_file(local_file_path=self.sample_file_location, s3_key="test/test_delete_suffix.txt")
        s3u.delete_suffix(key_prefix="test", suffix="txt")
        dirname = "./test_folder_3"
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        with self.assertRaises(ClientError):
            s3u.download_file(local_file_path=dirname + "/test_delete_suffix.txt",
                              s3_key="test/test_delete_suffix.txt")

    @mock_s3
    def test_should__save_dict__when_using_s3util(self):
        bucket = "TEST_BUCKET"
        conn = AwsConnection(mode="standard_env_var", region_name="ap-southeast-2", settings={})
        s3u = S3Util(conn=conn, bucket=bucket)
        s3u.create_bucket()
        upload_key = "test/test_json_sample.json"
        s3u.upload_file(local_file_path=self.sample_json_location, s3_key=upload_key)
        json_list = []
        json_list += [{
            "test_field": "test_value"
        }]
        s3u.save_dict(key=upload_key, json_list=json_list)
        redown_file = "{}re".format(self.sample_json_location)
        s3u.download_file(local_file_path=redown_file, s3_key=upload_key)
        with open(redown_file, 'r') as f:
            redown_content = f.read()
        expected = """
[
  {
    "test_field": "test_value"
  }
]        
        """.strip()
        self.assertEqual(expected, redown_content)

    @mock_s3
    def test_should__read_dict__when_using_s3util(self):
        bucket = "TEST_BUCKET"
        conn = AwsConnection(mode="standard_env_var", region_name="ap-southeast-2", settings={})
        s3u = S3Util(conn=conn, bucket=bucket)
        s3u.create_bucket()
        upload_key = "test/test_json_sample.json"
        s3u.upload_file(local_file_path=self.sample_json_location, s3_key=upload_key)
        json_list = []
        json_list += [{
            "test_field": "test_value"
        }]
        s3u.save_dict(key=upload_key, json_list=json_list)
        actual = json.dumps(s3u.read_dict(key=upload_key))
        expected = """[{"test_field": "test_value"}]"""
        self.assertEqual(expected, actual.strip())

    @mock_s3
    def test_should__read_all_lines__when_using_s3util(self):
        bucket = "TEST_BUCKET"
        conn = AwsConnection(mode="standard_env_var", region_name="ap-southeast-2", settings={})
        s3u = S3Util(conn=conn, bucket=bucket)
        s3u.create_bucket()
        upload_key = "test/test_json_sample.json"
        s3u.upload_file(local_file_path=self.sample_json_location, s3_key=upload_key)
        json_list = []
        json_list += [{
            "test_field": "test_value"
        }]
        s3u.save_dict(key=upload_key, json_list=json_list)
        actual = s3u.read_all_lines(key_prefix=upload_key)[2]
        expected = """ "test_field": "test_value" """.strip()
        self.assertEqual(expected, actual.strip())

    @mock_s3
    def test_should__get_all_keys__when_using_s3util(self):
        bucket = "TEST_BUCKET"
        conn = AwsConnection(mode="standard_env_var", region_name="ap-southeast-2", settings={})
        s3u = S3Util(conn=conn, bucket=bucket)
        s3u.create_bucket()
        upload_key = "test/test_json_sample.json"
        s3u.upload_file(local_file_path=self.sample_json_location, s3_key=upload_key)
        expected = upload_key
        result_list = s3u.get_all_keys(key_prefix=upload_key)
        self.assertEqual(1, len(result_list))
        self.assertEqual(expected, result_list[0])

    @mock_s3
    def test_should__upload_binary_stream__when_using_s3util(self):
        bucket = "TEST_BUCKET"
        conn = AwsConnection(mode="standard_env_var", region_name="ap-southeast-2", settings={})
        s3u = S3Util(conn=conn, bucket=bucket)
        s3u.create_bucket()
        upload_key = "test/binary_file.obj"
        binary_data = b"test data"
        s3u.upload_binary_stream(stream=binary_data, key=upload_key)
        s3u.download_file(local_file_path=self.sample_dir_location + "/binary_file.obj", s3_key=upload_key)
        with open(self.sample_dir_location + "/binary_file.obj", 'rb') as f:
            actual = f.read()
        self.assertEqual(actual, binary_data)

    @mock_s3
    def test_should__rename_file__when_using_s3util(self):
        bucket = "TEST_BUCKET"
        conn = AwsConnection(mode="standard_env_var", region_name="ap-southeast-2", settings={})
        s3u = S3Util(conn=conn, bucket=bucket)
        s3u.create_bucket()
        upload_key = "test/test_json_sample.json"
        s3u.upload_file(local_file_path=self.sample_json_location, s3_key=upload_key)
        s3u.rename_file(bucket_name=bucket, file_path=upload_key, new_name="test_json_sample_renamed.json")
        renamed_key = "test/test_json_sample_renamed.json"
        expected = renamed_key
        result_list = s3u.get_all_keys(key_prefix=renamed_key)
        self.assertEqual(expected, result_list[0])
