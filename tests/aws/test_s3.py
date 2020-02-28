import json
import os
import uuid
from unittest import TestCase

import pandas as pd
from botocore.exceptions import ClientError
from moto import mock_s3
from pandas.util.testing import assert_frame_equal

from hip_data_tools.aws.common import AwsConnectionManager, AwsConnectionSettings, AwsSecretsManager
from hip_data_tools.aws.s3 import S3Util, _multi_process_upload_file


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
        self.s3.upload_file(local_file_path=sample_file, key=upload_key)
        self.clean_test_files(sample_file)
        redown_file = f"{sample_file}re"
        self.s3.download_file(local_file_path=redown_file, key=upload_key)
        with open(redown_file, 'r') as f:
            redown_content = f.read()
        self.clean_test_files(redown_file)
        self.assertEqual(original_content, redown_content)

    @mock_s3
    def test_should__serialise_deserialise_file_to_from_s3__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "temp.pickle"
        test_object = {"this": "is good"}
        self.s3.serialise_and_upload_object(obj=test_object, key=upload_key)
        actual_object = self.s3.download_object_and_deserialise(key=upload_key)
        self.assertEqual(test_object, actual_object)

    @mock_s3
    def test_should__upload_dataframe_and_download_parquet__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "temp2.pickle"
        test_object = pd.DataFrame([1, 2, 3, 4], columns=["one"])
        self.s3.upload_dataframe_as_parquet(dataframe=test_object, key=upload_key)
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
        self.s3.upload_file(local_file_path=sample_file, key=upload_key)
        redown_file = f"{sample_file}re"
        self.s3.download_file(local_file_path=redown_file, key=upload_key)
        with open(redown_file, 'r') as f:
            redown_content = f.read()
        self.clean_test_files(redown_file)
        self.assertEqual(original_content, redown_content)

    @mock_s3
    def test_should__upload_then_download_file_from_s3__when_using_s3util(self):
        self.s3.create_bucket()
        sample_file = "./sample5.txt"
        upload_key = "test_sample_for_download.txt"
        sample_file_content = self.create_sample_file(sample_file)
        self.s3.upload_file(local_file_path=sample_file,
                            key=f"test/{upload_key}")
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
        self.s3.serialise_and_upload_object(obj=test_object, key=upload_key)
        actual_object = self.s3.download_object_and_deserialise(key=upload_key)
        self.assertEqual(test_object, actual_object)

    @mock_s3
    def test_should__upload_dataframe_and_download_parquet__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "temp456"
        test_object = pd.DataFrame([1, 2, 3, 4], columns=["one"])
        self.s3.upload_dataframe_as_parquet(dataframe=test_object, key=upload_key)
        redown_df = self.s3.download_parquet_as_dataframe(f"{upload_key}/data.parquet")
        assert_frame_equal(test_object, redown_df)

    @mock_s3
    def test_should__delete_recursive__when_using_s3util(self):
        self.s3.create_bucket()
        sample_file = "./sample6.txt"
        self.create_sample_file(sample_file)
        self.s3.upload_file(local_file_path=sample_file,
                            key="test6/test_delete_recursive.txt")
        self.s3.delete_recursive("test")
        with self.assertRaises(ClientError):
            self.s3.download_file(local_file_path="something", key="test6/")

    @mock_s3
    def test_should__delete_recursive_match_suffix__when_using_s3util(self):
        self.s3.create_bucket()
        sample_file = "./sample7.txt"
        self.create_sample_file(sample_file)
        self.s3.upload_file(local_file_path=sample_file,
                            key="test7/test_delete_suffix.txt")
        self.s3.delete_recursive_match_suffix(key_prefix="test", suffix="txt")
        with self.assertRaises(ClientError):
            self.s3.download_file(local_file_path="something",
                                  key="test7/")

    @mock_s3
    def test_should__save_dict__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "test_json_sample.json"
        json_list = []
        json_list += [{
            "test_field": "test_value"
        }]
        self.s3.upload_json(key=upload_key, json_list=json_list)
        self.s3.download_file(local_file_path=f"./{upload_key}", key=upload_key)
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
        self.s3.upload_json(key=upload_key, json_list=json_list)
        actual = json.dumps(self.s3.download_json(key=upload_key))
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
        self.s3.upload_json(key=upload_key, json_list=json_list)
        actual = self.s3.read_lines_as_list(key_prefix=upload_key)[2]
        expected = """ "test_field": "test_value" """.strip()
        self.assertEqual(expected, actual.strip())

    @mock_s3
    def test_should__get_all_keys__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "test/test_json_sample.json"
        json_list = [{
            "test_field": "test_value"
        }]
        self.s3.upload_json(key=upload_key, json_list=json_list)
        result_list = self.s3.get_keys(key_prefix=upload_key)
        self.assertEqual(1, len(result_list))
        self.assertEqual(upload_key, result_list[0])

    @mock_s3
    def test_should__upload_binary_stream__when_using_s3util(self):
        self.s3.create_bucket()
        upload_key = "test/binary_file.obj"
        binary_data = b"test data"
        self.s3.upload_binary_stream(stream=binary_data, key=upload_key)
        self.s3.download_file(local_file_path="./binary_file.obj",
                              key=upload_key)
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
        self.s3.upload_json(key=upload_key, json_list=json_list)
        self.s3.rename_file(key=upload_key, new_file_name=renamed)
        expected = f"test/{renamed}"
        result_list = self.s3.get_keys(key_prefix="test")
        self.assertEqual(expected, result_list[0])

    @mock_s3
    def test___multi_process_upload_file_works_like_upload_file(self):
        self.s3.create_bucket()
        sample_file = "./sample567.txt"
        self.create_sample_file(sample_file)
        self.s3.upload_file(
            local_file_path=sample_file,
            key="test9/compare.txt"
        )
        sample_file = "./sample568.txt"
        self.create_sample_file(sample_file)
        _multi_process_upload_file(
            settings=self.s3.conn.settings,
            filename=sample_file,
            bucket=self.s3.bucket,
            key="test10/compare.txt"
        )
        uplaoded = self.s3.get_keys('')
        print(f"got keys {uplaoded}")
        self.assertListEqual(uplaoded, ['test10/compare.txt', 'test9/compare.txt'])

    @mock_s3
    def test_should__copy_file_from_one_bucket_to_another__when_valid_locations_are_given(self):
        dest_bucket = "TEST_BUCKET_DEST"
        conn = AwsConnectionManager(
            AwsConnectionSettings(region="ap-southeast-2", secrets_manager=AwsSecretsManager(),
                                  profile=None))
        s3_util_for_destination = S3Util(conn=conn, bucket=dest_bucket)
        s3_util_for_source = self.s3

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
                                                              destination_bucket_name=dest_bucket,
                                                              destination_dir="{}/test_copy/".format(
                                                                  dest_bucket))
        actual = s3_util_for_destination.read_lines_as_list("test_copy")[0]

        expected = "Test file content"
        self.assertEquals(actual, expected)
