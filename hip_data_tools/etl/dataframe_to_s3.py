
from attr import dataclass
from pandas import DataFrame

from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util


@dataclass
class DataFrameToS3Settings:
    source_dataframe: DataFrame
    target_bucket: str
    target_key_prefix: str
    target_file_name: str
    target_connection_settings: AwsConnectionSettings


class DataFrameToS3:
    def __init__(self, settings: DataFrameToS3Settings):
        self.__settings = settings

    def _get_s3_util(self) -> S3Util:
        return S3Util(
            bucket=self.__settings.target_bucket,
            conn=AwsConnectionManager(self.__settings.target_connection_settings),
        )

    def upload(self) -> None:
        s3u = self._get_s3_util()
        s3u.upload_dataframe_as_parquet(
            dataframe=self.__settings.source_dataframe,
            key=self.__settings.target_key_prefix,
            file_name=self.__settings.target_file_name
        )
