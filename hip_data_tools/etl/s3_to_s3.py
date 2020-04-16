"""
Module to deal with data transfer from S3 to Cassandra
"""
from typing import Optional, List

from attr import dataclass

from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.common import LOG


@dataclass
class S3ToS3Settings:
    """S3 to Cassandra ETL settings"""
    source_bucket: str
    source_key_prefix: str
    suffix: Optional[str]
    target_bucket: str
    target_key_prefix: str
    connection_settings: AwsConnectionSettings


class S3ToS3:
    """
    Class to transfer objects from s3 to s3
    Args:
        settings (S3ToS3Settings): settings for the etl to be executed
    Eg:
    >>> etl = S3ToS3(
    ...        S3ToS3Settings(
    ...            source_bucket=source_bucket,
    ...            source_key_prefix="source/prefix",
    ...            suffix=None,
    ...            target_bucket=target_bucket,
    ...            target_key_prefix="target/prefix",
    ...            connection_settings=aws_setting
    ...        )
    ...    )
    """

    def __init__(self, settings: S3ToS3Settings):
        self.__settings = settings
        self._s3_util = None
        self._source_keys = None

    def _get_s3_util(self) -> S3Util:
        if self._s3_util is None:
            self._s3_util = S3Util(
                bucket=self.__settings.source_bucket,
                conn=AwsConnectionManager(self.__settings.connection_settings),
            )
        return self._s3_util

    def _get_target_key(self, source_key: str) -> str:
        file_name = source_key.split('/')[-1]
        return f"{self.__settings.target_key_prefix}/{file_name}"

    def list_source_files(self) -> List[str]:
        """
        List all eligible source keys
        Returns: List[str]
        """
        if self._source_keys is None:
            keys = self._get_s3_util().get_keys(self.__settings.source_key_prefix)
            if self.__settings.suffix:
                keys = [key for key in keys if key.endswith(self.__settings.suffix)]
            self._source_keys = keys
            LOG.info("Listed and cached %s source files", len(self._source_keys))
        return self._source_keys

    def transfer_file(self, source_key: str) -> None:
        """
        Transfer one source s3 key to target
        Args:
            source_key (str): source s3 key
        Returns: None
        """
        s3 = self._get_s3_util().get_client()
        copy_source = {
            'Bucket': self.__settings.source_bucket,
            'Key': source_key
        }
        target_key = self._get_target_key(source_key)
        LOG.info("Transferring Key s3://%s/%s to s3://%s/%s",
                 self.__settings.source_bucket,
                 source_key,
                 self.__settings.target_bucket,
                 target_key)
        s3.copy(copy_source, self.__settings.target_bucket, target_key)

    def transfer_all_files(self) -> None:
        """
        Transfer all source keys to target, sequentially
        Returns: None
        """
        for source_key in self.list_source_files():
            self.transfer_file(source_key)
