import json
import logging as log
import os
from hashlib import sha256

import pandas as pd
import numpy as np

from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.aws.common import CommonUtil

from pandas.io.json import json_normalize


class PandasUtil:
    def __init__(self, context, base_dir="/data"):
        self.base_dir = base_dir
        self.airflow_context = context

    def return_dataframe(self, data_frame, data_frame_name, s3_backed=True, destination_bucket="test-bucket"):
        path = CommonUtil(context=self.airflow_context).get_cache_path('{0}.pkl'.format(data_frame_name))
        data_frame.to_pickle(path)
        log.debug("Saved data frame at {0}".format(path))
        self.airflow_context['ti'].xcom_push(key=data_frame_name, value=path)
        print("uploading data from path {}".format(path))
        if s3_backed:
            destination_key = path.strip("/")
            S3Util(conn="s3_gandalf_write", bucket=destination_bucket).upload_file(
                local_file_path=path,
                s3_key=destination_key,
                remove_local=False)
            self.airflow_context['ti'].xcom_push(key=data_frame_name + "_s3bucket", value=destination_bucket)
            self.airflow_context['ti'].xcom_push(key=data_frame_name + "_s3key", value=destination_key)
        return path

    def retrive_dataframe(self, task_id, dataframe_name, s3_backed=True):
        path = self.airflow_context['ti'].xcom_pull(task_ids=task_id, key=dataframe_name)
        if s3_backed and (not os.path.isfile(path)):
            source_bucket = self.airflow_context['ti'].xcom_pull(task_ids=task_id, key=dataframe_name + "_s3bucket")
            source_key = self.airflow_context['ti'].xcom_pull(task_ids=task_id, key=dataframe_name + "_s3key")
            S3Util(conn="s3_gandalf_write", bucket=source_bucket).download_file(
                local_file_path=path,
                s3_key=source_key)
        df = pd.read_pickle(path, compression=None)
        print("Retrieved data frame of size :{0} from path {1}".format(df.shape[0], path))
        log.debug(df.head(2))
        return df

    def json_to_dataframe(self, data, sep="_"):
        """
        Creates a Data frame from a list of Json objects after flattening the json
        Args:
            data: list of Json objects
            sep (String): Absolute path to save file to local
        Returns: data frame
        """
        if len(data) > 0:
            result_df = json_normalize(data, sep=sep)
            return result_df
        else:
            return pd.DataFrame()

    def transform_column_dtypes(self, df, dtype_transformations):
        """
        transforms data frame by converting column data types
        Args:
            df:
            dtype_transformations:
        Returns: data frame
        """
        for col in df.columns:
            target_dtypes = dtype_transformations.get(col, None)
            if target_dtypes is not None:
                if target_dtypes in [np.int8, np.int32, np.int64, np.int]:
                    print("Substitute null values to 0 for column {}".format(col))
                    df[col] = df[col].fillna(0)
                print("Transforming column {} to {}".format(col, target_dtypes))
                df[col] = df[col].astype(target_dtypes)
        return df

    def transform_columnnames_lower(self, df):
        df.columns = map(str.lower, df.columns)
        return df

    def transform_column_as_json(self, df, column_list):
        """
        Transformation to convert complex struct data types into json strings, This is required while writing into
        Parquet as it does not support complex nested types
        Args:
            df:
            column_list:
        Returns: data frame
        """
        for col in df.columns:
            # Convert a complex nested type to json string
            if col in column_list:
                print("Transforming column {} to json String".format(col))
                df[col] = df[col].apply(lambda x: str(json.dumps(x)))
        return df

    def transform_remove_pii(self, df):
        """
        Replaces Email Addresses with a sha256 hashed version
        Args:
            df:
        Returns: data frame
        """
        if "EmailAddress" in df.columns:
            df['SHA256EmailAddress'] = df.apply(
                lambda x: sha256(x.EmailAddress.strip().lower().encode('utf-8')).hexdigest(), axis=1)
            df.drop("EmailAddress", inplace=True, axis=1)
        return df

    def transform_date_datatype(self, df):
        """
        Transforms all date columns to the correct datatype
        Args:
            df:
        Returns: data frame
        """
        if "Date" in df.columns:
            df["Date"] = df["Date"].astype('datetime64[ns]')
        return df
