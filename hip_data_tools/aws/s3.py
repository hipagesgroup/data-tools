"""
Utility to connect to, and interact with the s3 file storage system
"""
import logging as log
import uuid
from pathlib import Path
from multiprocessing import Pool
import os
import json

import boto3
import arrow
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

    def upload_file(self, local_file_path, s3_key, remove_local=False):
        """
        Uploads a file from local to s3
        Args:
            local_file_path (string): Absolute local path to the file to upload
            s3_key (string): Absolute path within the s3 buck to upload the file
            remove_local: If this is true remove the local file
        Returns: None
        """
        s3 = self.conn.client(self.boto_type)
        s3.upload_file(local_file_path, self.bucket, s3_key)
        if remove_local:
            os.remove(local_file_path)

    def upload_directory(self, source_directory, extension, target_key, overwrite=True, rename=True):
        """
        Upload a local file directory to s3
        (NOT TESTED)
        Args:
            source_directory (string): Local source directory's absolute path
            extension (string): the file extension of files in that directory to be uploaded
            target_key (string): Target location on the s3 bucket for files to be uploaded
            overwrite (Boolean): Boolean value to overwrite files on s3 or not
            rename (Boolean): Boolean value to rename the file when uploading to s3 or not
        Returns: NA
        """
        if overwrite:
            print("Cleaning existing files on s3")
            self.delete_recursive("{}/".format(target_key))
        print("searching for files to upload in {}".format(source_directory))
        path_list = Path(source_directory).glob('**/*.{ext}'.format(ext=extension))
        itr = 0
        upload_data = []
        for path in path_list:
            # because path is object not string
            path_in_str = str(path)
            filename = os.path.basename(path_in_str)

            if rename:
                filename = "file-{seq}.{ext}".format(seq=str(uuid.uuid4()), ext=extension)

            destination_key = "{dir}/{filename}".format(
                dir=target_key,
                filename=filename)

            itr = itr + 1
            upload_data += [(self.conn, path_in_str, self.bucket, destination_key)]
        pool_size = min(16, max(1, int(len(upload_data) / 3)))  # limit pool size between 1 and 16
        log.debug("uploading with a multiprocessing pool of %s processes", pool_size)

        Pool(pool_size).starmap(upload_file, upload_data)
        log.debug("Saved csv chunks at s3://%s/%s", self.bucket, target_key)

    def download_directory(self, source_key, file_suffix, local_directory):
        """
        Download an entire directory from s3 onto local file system
        Args:
            source_key (string): key prefix of the directory to be downloaded from s3
            file_suffix (string): suffix to sunset the files to be downloaded
            local_directory (string): local absolute path to store all the files
        Returns: NA
        """
        s3 = boto3.resource(self.boto_type)
        print("Downloading s3://{bucket}/{key} to {source}".format(
            source=local_directory,
            bucket=self.bucket,
            key=source_key))
        for obj in s3.Bucket(self.bucket).objects.filter(Prefix=source_key):
            key_path = obj.key.split("/")
            if obj.key.endswith(file_suffix):
                filename = "{}/{}".format(local_directory, key_path[-1])
                self.download_file(
                    local_file_path=filename,
                    s3_key=obj.key)

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
            engine:
            columns:
        Returns: DataFrame
        """
        random_tmp_file_nm = _generate_random_file_name()
        self.download_file(random_tmp_file_nm, s3_key)
        return pd.read_parquet(random_tmp_file_nm, engine=engine, columns=columns, **kwargs)

    def move_recursive_to_different_bucket(self, source_dir, destination_bucket_name, destination_dir,
                                           delete_after_copy=True, file_suffix_filter='None'):
        """
        Move files from one bucket to another
        Args:
            source_dir (str):
            destination_bucket_name (str):
            destination_dir (str):
            delete_after_copy (str): If True source files will be deleted after copying to destination
            file_suffix_filter (str): Filter out the files with this suffix
        Returns: NA
        """
        s3 = boto3.resource(self.boto_type)
        source_bucket = s3.Bucket(self.bucket)
        destination_bucket = s3.Bucket(destination_bucket_name)
        for obj in source_bucket.objects.filter(Prefix=source_dir):
            if not obj.key.endswith(file_suffix_filter):
                new_key = "{destination_dir_without_bucket_name}/{destination_file_name}".format(
                    destination_dir_without_bucket_name=destination_dir.replace(destination_bucket_name + '/', ''),
                    destination_file_name=obj.key.split('/')[-1])
                log.info("Moving s3 object from : \n%s \nto: \n%s", obj.key, new_key)
                new_obj = destination_bucket.Object(new_key)
                new_obj.copy({"Bucket": self.bucket, "Key": obj.key})
        if delete_after_copy:
            self.delete_recursive(source_dir)

    def create_bucket(self):
        """
        Creates the s3 bucket
        Returns: None
        """
        self.conn.resource(self.boto_type).create_bucket(Bucket=self.bucket)

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

    def delete_recursive(self, key_prefix, add_trailing_slash=False):
        """
        Recursively delete all keys with given prefix from the named bucket
        Args:
            key_prefix (str): Key prefix under which all files will be deleted
            add_trailing_slash: boolean flag indicating if a trailing slash should be added in order specify deletion
                of bucket folders rather than prefixes
        Returns: NA
        """
        target_prefix = "{key}{trailing_slash}".format(key=key_prefix, trailing_slash="/" if add_trailing_slash else "")
        log.info("Recursively deleting s3://%s/%s", self.bucket, target_prefix)
        s3 = boto3.resource(self.boto_type)
        response = s3.Bucket(self.bucket).objects.filter(Prefix=target_prefix).delete()
        log.info(response)

    def delete_suffix(self, key_prefix, suffix):
        """
        Recursively delete all keys with given key prefix and suffix from the bucket
        Args:
            key_prefix (str): Key prefix under which all files will be deleted
            suffix (str): suffix of the subset of files in the given prefix directory to be deleted
        Returns: NA
        """
        if not suffix:
            raise ValueError("suffix must not be empty")
        s3 = boto3.resource(self.boto_type)
        for obj in s3.Bucket(self.bucket).objects.filter(Prefix=key_prefix):
            log.info(obj.key)
            if obj.key.endswith(suffix):
                log.info("deleting s3://%s/%s", self.bucket, obj.key)
                response = obj.delete()
                log.info(response)

    def copy_files(self, source_key, destination_conn_id, destination_bucket, destination_key, local_directory="/tmp",
                   extension="gz"):
        """
        A method to copy an entire key from a bucket that exists in one connection to another
        (NOT TESTED)
        Args:
            source_key:
            destination_conn_id:
            destination_bucket:
            destination_key:
            local_directory:
            extension:
        Returns: NA
        """
        print(
            "Copying from {source_conn_id} s3://{source_bucket}/{source_key} to {destination_conn_id} s3://{destination_bucket}/{destination_key}".format(
                source_conn_id=self.conn,
                source_bucket=self.bucket,
                source_key=source_key,
                destination_conn_id=destination_conn_id,
                destination_bucket=destination_bucket,
                destination_key=destination_key
            ))

        self.download_directory(source_key=source_key, file_suffix=extension, local_directory=local_directory)

        destination_s3 = S3Util(conn=destination_conn_id, bucket=destination_bucket)
        destination_s3.upload_directory(source_directory=local_directory, extension=extension,
                                        target_key=destination_key, overwrite=False, rename=False)

    def save_dict(self, key, json_list):
        """
        Save the json/dict data structure onto s3 as a file without using temporary local files
        Args:
            key (str): target key of the file on s3
            json_list (dictionary): a list of dictionaries that are saved as newline json in a file
        Returns: NA
        """
        s3 = boto3.resource(self.boto_type)
        s3.Object(self.bucket, key).put(
            Body=(bytes(json.dumps(json_list, indent=2).encode('UTF-8')))
        )

    def read_dict(self, key):
        """
        Read a file with json in a file on s3
        Args:
            key (str): target key of the file on s3
        Returns: json data read from file
        """
        s3 = boto3.resource(self.boto_type)
        json_content = json.loads(s3.Object(self.bucket, key).get()['Body'].read().decode('utf-8'))
        return json_content

    def read_modified_lines(self, key_prefix, start_date, end_date):
        """
        Read lines from s3 files which have changed between the given date time ranges
        (NOT TESTED)
        Args:
            key_prefix (str): the key prefix under which all files will be sensed
            start_date: arrow datetime object
            end_date: arrow datetime object
        Returns: a list of strings representing lines read from modified files
        """
        s3 = boto3.resource(self.boto_type)
        lines = []
        print("reading files from s3://{bucket}/{key} \n between {start_date} to {end_date}".format(
            bucket=self.bucket, key=key_prefix, start_date=start_date, end_date=end_date))
        file_list = self.get_changed_keys(key_prefix, start_date, end_date)
        for file in file_list:
            self.__update_lines(s3=s3, key=file, lines=lines)
        # Flatten the list of lists
        flat_lines = [item for sublist in lines for item in sublist]
        print("read {} lines from {} s3 files".format(len(flat_lines), len(lines)))
        return flat_lines

    def read_all_lines(self, key_prefix):
        """
        Read lines from s3 files
        Args:
            key_prefix (str):  the key prefix under which all files will be read
        Returns: a list of strings representing lines read from all files
        """
        s3 = boto3.resource(self.boto_type)
        bucket = s3.Bucket(name=self.bucket)
        lines = []
        print("reading files from s3://{bucket}/{key} ".format(bucket=self.bucket, key=key_prefix))
        file_metadata = bucket.objects.filter(Prefix=key_prefix)
        for file in file_metadata:
            self.__update_lines(s3=s3, key=file.key, lines=lines)
        # Flatten the list of lists
        flat_lines = [item for sublist in lines for item in sublist]
        print("read {} lines from {} s3 files".format(len(flat_lines), len(lines)))
        return flat_lines

    def get_changed_keys(self, key_prefix, start_date, end_date):
        """
        Sense if there were any files changed or added in the given time period under the given key prefix and return a
        list of keys
        (NOT TESTED)
        Args:
            key_prefix (str): the key prefix under which all files will be sensed
            start_date: arrow datetime object
            end_date: arrow datetime object
        Returns: a list of keys which were modified
        """
        print("sensing files from s3://{bucket}/{key} \n between {start_date} to {end_date}".format(
            bucket=self.bucket, key=key_prefix, start_date=start_date, end_date=end_date))
        metadata = self.get_object_metadata(key_prefix)
        lines = []
        for file in metadata:
            if start_date < arrow.get(file.last_modified) <= end_date:
                lines += [file.key]
        print("found {} s3 files changed".format(len(lines)))
        return lines

    def get_all_keys(self, key_prefix):
        """
        Sense all keys under a given key prefix
        Args:
            key_prefix (str):  the key prefix under which all files will be sensed
        Returns: a list of keys which were modified
        """
        print("sensing files from s3://{bucket}/{key} ".format(bucket=self.bucket, key=key_prefix))
        metadata = self.get_object_metadata(key_prefix)
        lines = []
        for file in metadata:
            lines += [file.key]
        print("found {} s3 files changed".format(len(lines)))
        return lines

    def get_object_metadata(self, key_prefix):
        """
        Get metadata for all objects under a key prefix
        Args:
            key_prefix (str):  the key prefix under which all files will be sensed
        Returns: a list of object metadata
        """
        s3 = boto3.resource(self.boto_type)
        bucket = s3.Bucket(name=self.bucket)
        metadata = bucket.objects.filter(Prefix=key_prefix)
        return metadata

    def upload_binary_stream(self, stream, key):
        """
        Upload binary stream to S3
        Args:
            stream (binary):  binary data
            key (string): s3 file key
        Returns: NA
        """
        s3 = boto3.resource(self.boto_type)
        s3_object = s3.Object(self.bucket, key)
        object.put(Body=s3_object)

    def rename_file(self, bucket_name, file_path, new_name):
        """
        This method can be used to rename any file in S3
        Args:
            bucket_name (string):  Name of the bucket
            file_path (string): Path of the existing file including the file name and extension
            new_name (string): The new name of the file
        Returns: NA
        """
        s3 = boto3.resource(self.boto_type)
        full_new_file_path = file_path.rpartition('/')[0] + '/' + new_name
        print("Renaming source: " + file_path)
        print("Renaming destination: " + full_new_file_path)
        s3.Object(bucket_name, full_new_file_path).copy_from(CopySource={'Bucket': bucket_name, 'Key': file_path})
        s3.Object(bucket_name, file_path).delete()

    def get_the_bucket(self, bucket_name):
        """
        Return the bucket using the given bucket name
        Args:
            bucket_name (str):  name of the bucket
        Returns: S3 bucket
        """
        s3 = boto3.resource(self.boto_type)
        return s3.Bucket(bucket_name)

    def __update_lines(self, s3, key, lines):
        obj = s3.Object(self.bucket, key)
        data = obj.get()['Body'].read().decode('utf-8')
        lines.append(data.splitlines())


def upload_file(conn, file_path, destination_bucket, destination_key, boto_type='s3'):
    """
    Uploads a file from local to s3
    Args:
        conn:
        boto_type:
        file_path (string): Absolute local path to the file to upload
        destination_bucket: bucket name
        destination_key (string): s3 key
    Returns: None
    """
    s3 = conn.client(boto_type)
    absolute_s3_path = "s3://{destination_bucket}/{destination_key}".format(destination_bucket=destination_bucket,
                                                                            destination_key=destination_key)
    s3.upload_file(file_path, destination_bucket, absolute_s3_path)
