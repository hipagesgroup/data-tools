"""
Utility to connect to, and interact with the s3 file storage system
"""
import logging as log
import uuid

import boto3
import pandas as pd
from joblib import load, dump

from hip_data_tools.common import _generate_random_file_name


class S3Util:
    """
    Utility class for connecting to s3 and manipulate data in a pythonic way
    Args:
        conn (AwsConnection): AwsConnection object or a boto.Session object
        bucket (string): S3 bucket name where these operations will take place
    """

    def __init__(self, conn, bucket):
        self.conn = conn
        self.bucket = bucket
        self.boto_type = "s3"

    def download_file(self, local_file_path, s3_key):
        """
        Downloads a file from a path on s3 to a local path on disk
        Args:
            local_file_path (string): Absolute path on s3
            s3_key (string): Absolute path to save file to local
        Returns: None
        """
        s3 = self.conn.client(self.boto_type)

        s3.download_file(self.bucket, s3_key, local_file_path)

    def upload_file(self, local_file_path, s3_key):
        """
        Uploads a file from local to s3
        Args:
            local_file_path (string): Absolute local path to the file to upload
            s3_key (string): Absolute path within the s3 buck to upload the file
        Returns: None
        """
        s3 = self.conn.client(self.boto_type)
        s3.upload_file(local_file_path, self.bucket, s3_key)

    def download_object_and_deserialse(self, s3_key, local_file_path=None):
        """
        Download a serialised object from S3 and deserialse
        Args:
            s3_key (string): Absolute path on s3 to the file
            local_file_path (string): The deserialsed object
        Returns: object
        """
        if local_file_path is None:
            local_file_path = "/tmp/tmp_file{}".format(str(uuid.uuid4()))

        self.download_file(s3_key=s3_key, local_file_path=local_file_path)
        return load(local_file_path)

    def serialise_and_upload_object(self, obj, s3_key):
        """
        Serialise any object to disk, and then upload to S3
        Args:
            obj (object): Any serialisable object
            s3_key (string): The absolute path on s3 to upload the file to
        Returns: None
        """

        random_tmp_file_nm = _generate_random_file_name()
        dump(obj, random_tmp_file_nm)
        self.upload_file(local_file_path=random_tmp_file_nm, s3_key=s3_key)

    def create_bucket(self):
        """
        Creates the s3 bucket
        Returns: None
        """
        self.conn.resource(self.boto_type).create_bucket(Bucket=self.bucket)

    def upload_df_parquet(self, df, s3_key):
        """
        Exports a datafame to a parquet file on s3
        Args:
            df (DataFrame): dataframe to export
            s3_key (str): The absolute path on s3 to upload the file to
        Returns: None
        """
        random_tmp_file_nm = _generate_random_file_name()
        df.to_parquet(random_tmp_file_nm)
        self.upload_file(random_tmp_file_nm, s3_key)

    def download_df_parquet(self, s3_key, engine='auto', columns=None, **kwargs):
        """
        Exports a datafame to a parquet file on s3
        Args:
            s3_key (str): The absolute path on s3 to upload the file to
        Returns: DataFrame
        """
        random_tmp_file_nm = _generate_random_file_name()
        self.download_file(random_tmp_file_nm, s3_key)
        return pd.read_parquet(random_tmp_file_nm, engine=engine, columns=columns, **kwargs)

    def move_recursive_to_different_bucket(self, source_dir, destination_bucket_name,
                                           destination_dir,
                                           delete_after_copy=True, file_suffix_filter='None'):
        """
        Move files from one bucket to another
        Args:
            source_dir (str):
            destination_bucket_name (str):
            destination_dir (str):
            delete_after_copy (str): If True source files will be deleted after copying to
            destination
            file_suffix_filter (str): Filter out the files with this suffix
        Returns: NA
        """
        s3 = boto3.resource(self.boto_type)
        source_bucket = s3.Bucket(self.bucket)
        destination_bucket = s3.Bucket(destination_bucket_name)
        for obj in source_bucket.objects.filter(Prefix=source_dir):
            if not obj.key.endswith(file_suffix_filter):
                new_key = "{destination_dir_without_bucket_name}/{destination_file_name}".format(
                    destination_dir_without_bucket_name=destination_dir.replace(
                        destination_bucket_name + '/', ''),
                    destination_file_name=obj.key.split('/')[-1])
                log.info("Moving s3 object from : \n%s \nto: \n%s", obj.key, new_key)
                new_obj = destination_bucket.Object(new_key)
                new_obj.copy({"Bucket": self.bucket, "Key": obj.key})
        if delete_after_copy:
            self.delete_recursive(source_dir)

    def read_lines_as_list(self, key_prefix_filter):
        """
        Read lines from s3 files
        Args:
            key_prefix_filter (str): the key prefix under which all files will be read
        Returns: a list of strings representing lines read from all files
        """
        s3 = boto3.resource(self.boto_type)
        bucket = s3.Bucket(name=self.bucket)
        lines = []
        log.info("reading files from s3://%s/%s ", self.bucket, key_prefix_filter)
        file_metadata = bucket.objects.filter(Prefix=key_prefix_filter)
        for file in file_metadata:
            obj = s3.Object(self.bucket, file.key)
            data = obj.get()["Body"].read().decode("utf-8")
            lines.append(data.splitlines())
        # Flatten the list of lists
        flat_lines = [item for sublist in lines for item in sublist]
        log.info("Read %d lines from %d s3 files", len(flat_lines), len(lines))
        return flat_lines

    def delete_recursive(self, key_prefix):
        """
        Recursively delete all keys with given prefix from the named bucket
        Args:
            key_prefix (str): Key prefix under which all files will be deleted
        Returns: NA
        """
        log.info("Recursively deleting s3://%s/%s", self.bucket, key_prefix)
        s3 = boto3.resource(self.boto_type)
        response = s3.Bucket(self.bucket).objects.filter(Prefix=key_prefix).delete()
        log.info(response)
