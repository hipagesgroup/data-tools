"""
Module to deal with data transfer from S3 to Pandas DataFrame
"""
from typing import Iterator, List

import pandas as pd
from attr import dataclass
from pandas import DataFrame

from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.common import LOG


@dataclass
class S3ToDataFrameSettings:
    """S3 to DataFrame ETL settings"""
    source_bucket: str
    source_key_prefix: str
    source_connection_settings: AwsConnectionSettings


class S3ToDataFrame:
    """
    Class to transfer parquet data from s3 to Pandas DataFrame
    Args:
        settings (S3ToDataFrameSettings): the settings around the etl to be executed
    """

    def __init__(self, settings: S3ToDataFrameSettings):
        self._iterator = None
        self.__settings = settings
        self.keys_to_transfer = None
        self._processed_counter = 0

    def __iter__(self):
        self._processed_counter = 0
        return self

    def __next__(self) -> DataFrame:
        source_files = self.list_source_files()
        if source_files:
            data = self.get_data_frame(source_files[self._processed_counter])
            self._processed_counter += 1
            return data
        else:
            return DataFrame()

    def _get_s3_util(self) -> S3Util:
        return S3Util(
            bucket=self.__settings.source_bucket,
            conn=AwsConnectionManager(self.__settings.source_connection_settings),
        )

    def _get_iterator(self) -> Iterator:
        if self._iterator is None:
            self._iterator = iter(self)
        return self._iterator

    def get_all_files_as_data_frame(self) -> DataFrame:
        """
        Downloads and collates all files in a given s3 dir and returns a single DataFrame
        Returns: DataFrame
        """
        return pd.concat([self.get_data_frame(key) for key in self.list_source_files()])

    def get_data_frame(self, key: str) -> DataFrame:
        """
        Read a parquet file from s3 and convert it to a parquet DataFrame
        Args:
            key: s3 key for the parquet file
        Returns: None
        """
        return self._get_s3_util().download_parquet_as_dataframe(key=key)

    def list_source_files(self) -> List[str]:
        """
        Lists all the files that are encompassed under the s3 location in settings
        Returns: list[str]
        """
        if self.keys_to_transfer is None:
            self.keys_to_transfer = self._get_s3_util().get_keys(
                self.__settings.source_key_prefix)
            LOG.info("Listed and cached %s source files", len(self.keys_to_transfer))
        return self.keys_to_transfer

    def next(self) -> DataFrame:
        """
        Gets the next DataFrame from the next file on s3.

        Please note, if you are trying to run window functions or operations on the data set that
        spans multiple rows, then using this method may result in incorrect or inaccurate
        results. For such use cases, use `get_all_files_as_data_frame()`
        Returns: DataFrame
        """
        return next(self._get_iterator())
