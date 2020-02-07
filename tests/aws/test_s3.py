import json
import os
import uuid
from unittest import TestCase

import pandas as pd
from botocore.exceptions import ClientError
from moto import mock_s3
from pandas.util.testing import assert_frame_equal

from hip_data_tools.aws.common import AwsConnectionManager, AwsConnectionSettings, AwsSecretsManager
from hip_data_tools.aws.s3 import S3Util


class TestS3Util(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bucket = "TEST_BUCKET"
        conn = AwsConnectionManager(
            AwsConnectionSettings(region="ap-southeast-2", secrets_manager=AwsSecretsManager(),
                                  profile=None))
        cls.s3 = S3Util(conn=conn, bucket=cls.bucket)

    def create_sample_file(self, file):
        sample_file_content = str(uuid.uuid4())
        with open(file, 'w+') as f:
            f.write(sample_file_content)
        return sample_file_content

    def clean_test_files(self, file):
        os.remove(file)

    @mock_s3
    def test_should__upload_then_download_file_from_s3__when_using_s3util(self):
        self.s3.create_bucket()
        sample_file = "./sample1.txt"
        original_content = self.create_sample_file(sample_file)
        upload_key = "temp.txt"
        self.s3.upload_file(local_file_path=sample_file, s3_key=upload_key)
        self.clean_test_files(sample_file)
        redown_file = f"{sample_file}re"
        self.s3.download_file(local_file_path=redown_file, s3_key=upload_key)
        with open(redown_file, 'r') as f:
            redown_content = f.read()
        self.clean_test_files(redown_file)
        self.assertEqual(original_content, redown_content)

    @mock_s3
    def test_should__serialise_deserialise_file_to_from_s3__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "temp.pickle"
        test_object = {"this": "is good"}
        self.s3.serialise_and_upload_object(obj=test_object, s3_key=upload_key)
        actual_object = self.s3.download_object_and_deserialise(s3_key=upload_key)
        self.assertEqual(test_object, actual_object)

    @mock_s3
    def test_should__upload_dataframe_and_download_parquet__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "temp2.pickle"
        test_object = pd.DataFrame([1, 2, 3, 4], columns=["one"])
        self.s3.upload_dataframe_as_parquet(dataframe=test_object, s3_key=upload_key)
        redown_df = self.s3.download_parquet_as_dataframe(upload_key)
        assert_frame_equal(test_object, redown_df)

    @mock_s3
    def test__list_objects_should_provide_a_complete_list(self):
        self.s3.create_bucket()
        sample_file = "./sample8765.txt"
        self.create_sample_file(sample_file)
        for itr in range(1500):
            self.s3.upload_file(sample_file, f"some/{itr}.abc", remove_local=False)
        self.clean_test_files(sample_file)
        keys = self.s3.get_keys("some")
        self.assertEqual(len(keys), 1500)
        keys = self.s3.get_keys("some/111")
        self.assertEqual(len(keys), 11)

    @mock_s3
    def test_should__upload_then_download_file_from_s3__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "temp.txt"
        sample_file = "./sample4.txt"
        original_content = self.create_sample_file(sample_file)
        self.s3.upload_file(local_file_path=sample_file, s3_key=upload_key)
        redown_file = f"{sample_file}re"
        self.s3.download_file(local_file_path=redown_file, s3_key=upload_key)
        with open(redown_file, 'r') as f:
            redown_content = f.read()
        self.clean_test_files(redown_file)
        self.assertEqual(original_content, redown_content)

    @mock_s3
    def test_should__upload_then_download_directory_from_s3__when_using_s3util(self):
        self.s3.create_bucket()
        sample_file = "./sample5.txt"
        upload_key = "test_sample_for_download.txt"
        sample_file_content = self.create_sample_file(sample_file)
        self.s3.upload_file(local_file_path=sample_file,
                            s3_key=f"test/{upload_key}")
        self.s3.download_directory(source_key="test", file_suffix=".txt",
                                   local_directory=".")
        with open(upload_key, 'r') as f:
            redown_content = f.read()
        self.clean_test_files(upload_key)
        self.assertEqual(sample_file_content, redown_content)

    @mock_s3
    def test_should__serialise_deserialise_file_to_from_s3__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "temp.pickle"
        test_object = {"this": "is good"}
        self.s3.serialise_and_upload_object(obj=test_object, s3_key=upload_key)
        actual_object = self.s3.download_object_and_deserialise(s3_key=upload_key)
        self.assertEqual(test_object, actual_object)

    @mock_s3
    def test_should__upload_dataframe_and_download_parquet__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "temp456"
        test_object = pd.DataFrame([1, 2, 3, 4], columns=["one"])
        self.s3.upload_dataframe_as_parquet(dataframe=test_object, s3_key=upload_key)
        redown_df = self.s3.download_parquet_as_dataframe(f"{upload_key}/data.parquet")
        assert_frame_equal(test_object, redown_df)

    @mock_s3
    def test_should__delete_recursive__when_using_s3util(self):
        self.s3.create_bucket()
        sample_file = "./sample6.txt"
        self.create_sample_file(sample_file)
        self.s3.upload_file(local_file_path=sample_file,
                            s3_key="test6/test_delete_recursive.txt")
        self.s3.delete_recursive("test")
        with self.assertRaises(ClientError):
            self.s3.download_file(local_file_path="something", s3_key="test6/")

    @mock_s3
    def test_should__delete_recursive_match_suffix__when_using_s3util(self):
        self.s3.create_bucket()
        sample_file = "./sample7.txt"
        self.create_sample_file(sample_file)
        self.s3.upload_file(local_file_path=sample_file,
                            s3_key="test7/test_delete_suffix.txt")
        self.s3.delete_recursive_match_suffix(s3_key_prefix="test", suffix="txt")
        with self.assertRaises(ClientError):
            self.s3.download_file(local_file_path="something",
                                  s3_key="test7/")

    @mock_s3
    def test_should__save_dict__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "test_json_sample.json"
        json_list = []
        json_list += [{
            "test_field": "test_value"
        }]
        self.s3.upload_json(s3_key=upload_key, json_list=json_list)
        self.s3.download_file(local_file_path=f"./{upload_key}", s3_key=upload_key)
        with open(upload_key, 'r') as f:
            redown_content = f.read()
        self.clean_test_files(upload_key)
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
        self.s3.create_bucket()
        upload_key = "test/test_json_sample.json"
        json_list = []
        json_list += [{
            "test_field": "test_value"
        }]
        self.s3.upload_json(s3_key=upload_key, json_list=json_list)
        actual = json.dumps(self.s3.download_json(s3_key=upload_key))
        expected = """[{"test_field": "test_value"}]"""
        self.assertEqual(expected, actual.strip())

    @mock_s3
    def test_should__read_all_lines__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "test/test_json_sample.json"
        json_list = []
        json_list += [{
            "test_field": "test_value"
        }]
        self.s3.upload_json(s3_key=upload_key, json_list=json_list)
        actual = self.s3.read_lines_as_list(s3_key_prefix=upload_key)[2]
        expected = """ "test_field": "test_value" """.strip()
        self.assertEqual(expected, actual.strip())

    @mock_s3
    def test_should__get_all_keys__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "test/test_json_sample.json"
        json_list = [{
            "test_field": "test_value"
        }]
        self.s3.upload_json(s3_key=upload_key, json_list=json_list)
        result_list = self.s3.get_keys(s3_key_prefix=upload_key)
        self.assertEqual(1, len(result_list))
        self.assertEqual(upload_key, result_list[0])

    @mock_s3
    def test_should__upload_binary_stream__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "test/binary_file.obj"
        binary_data = b"test data"
        self.s3.upload_binary_stream(stream=binary_data, key=upload_key)
        self.s3.download_file(local_file_path="./binary_file.obj",
                              s3_key=upload_key)
        with open("./binary_file.obj", 'rb') as f:
            actual = f.read()
        self.clean_test_files("./binary_file.obj")
        self.assertEqual(actual, binary_data)

    @mock_s3
    def test_should__rename_file__when_using_s3util(self):
        self.s3.create_bucket()
        json_list = [{
            "test_field": "test_value"
        }]
        upload_key = "test/test_json_sample.json"
        renamed = "test_json_sample_renamed.json"
        self.s3.upload_json(s3_key=upload_key, json_list=json_list)
        self.s3.rename_file(s3_key=upload_key, new_file_name=renamed)
        expected = f"test/{renamed}"
        result_list = self.s3.get_keys(s3_key_prefix="test")
        self.assertEqual(expected, result_list[0])
