"""
Module tohandle upload of DataFrame to S3
"""
from attr import dataclass
from pandas import DataFrame

from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util


@dataclass
class DataFrameToS3Settings:
    """
    Settings data class to handle the configuration around upload of DataFrame to S3
    """
    source_dataframe: DataFrame
    target_bucket: str
    target_key_prefix: str
    target_file_name: str
    target_connection_settings: AwsConnectionSettings


class DataFrameToS3:
    """
    Class that uploads a given dataframe to S3
    example usage:

    >>> aws_setting = AwsConnectionSettings(
    ...     region="ap-southeast-2",
    ...     secrets_manager=AwsSecretsManager(),
    ...     profile=None)
    >>>
    >>> etl = DataFrameToS3(
    ...     DataFrameToS3Settings(
    ...         source_dataframe=DataFrame(),
    ...         target_bucket=target_bucket,
    ...         target_key_prefix="foo/bar/my/dir",
    ...         target_file_name="data",
    ...         target_connection_settings=aws_setting
    ...     )
    ... )
    >>>
    >>> etl.upload()

    Args:
        settings (DataFrameToS3Settings): Settings to handle the upload
        
    """
    def __init__(self, settings: DataFrameToS3Settings):
        self.__settings = settings

    def _get_s3_util(self) -> S3Util:
        return S3Util(
            bucket=self.__settings.target_bucket,
            conn=AwsConnectionManager(self.__settings.target_connection_settings),
        )

    def upload(self) -> None:
        """
        Uploads a given dataframe to the s3 location specified in settings

        Returns: None

        """
        s3u = self._get_s3_util()
        s3u.upload_dataframe_as_parquet(
            dataframe=self.__settings.source_dataframe,
            key=self.__settings.target_key_prefix,
            file_name=self.__settings.target_file_name
        )
