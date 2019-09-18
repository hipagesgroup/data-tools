"""
Utility to connect to, and interact with the s3 file storage system
"""
import uuid

import pandas as pd
from joblib import load, dump

from common import _generate_random_file_name


class S3Util:
    """
    Utility class for connecting to s3 and manipulate data in a pythonic way
    Args:
        conn (AwsConnection): AwsConnection object
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
