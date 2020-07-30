from abc import abstractmethod
from typing import Optional, List, Tuple, Any, NewType

from pandas import DataFrame

from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.common import LOG
from hip_data_tools.etl.common import SourceSettings, Extractor, SinkSettings, Loader


class S3SourceSettings(SourceSettings):
    source_bucket: str
    source_key_prefix: str
    suffix: Optional[str]
    connection_settings: AwsConnectionSettings


S3Key = NewType("S3Key", str)
FileName = NewType("FileName", str)
S3Bucket = NewType("S3Bucket", str)


class S3Extractor(Extractor):
    def __init__(self, settings: S3SourceSettings):
        super().__init__(settings)
        self.settings = settings
        self._s3_util = None

    def _get_s3_util(self) -> S3Util:
        if self._s3_util is None:
            self._s3_util = S3Util(
                bucket=self.settings.source_bucket,
                conn=AwsConnectionManager(self.settings.connection_settings),
            )
        return self._s3_util

    @abstractmethod
    def extract_next(self):
        pass

    @abstractmethod
    def has_next(self) -> bool:
        pass


class S3FilesExtractor(S3Extractor):
    def __init__(self, settings: S3SourceSettings):
        super().__init__(settings)
        self._source_keys = None
        self.file_counter = 0

    def _list_source_files(self) -> List[str]:
        if self._source_keys is None:
            keys = self._get_s3_util().get_keys(self.settings.source_key_prefix)
            if self.settings.suffix:
                keys = [key for key in keys if key.endswith(self.settings.suffix)]
            self._source_keys = keys
            LOG.info("Listed and cached %s source files", len(self._source_keys))
            self.file_counter = len(self._source_keys)
        return self._source_keys

    def _next_file_path(self) -> str:
        self.file_counter -= 1
        return self._list_source_files()[self.file_counter]

    @abstractmethod
    def extract_next(self) -> Tuple[str, str]:
        pass

    def has_next(self) -> bool:
        return self.file_counter > 0


class S3FileLocationExtractor(S3FilesExtractor):
    def __init__(self, settings: S3SourceSettings):
        super().__init__(settings)

    def extract_next(self) -> Tuple[str, str]:
        """
        next eligible source coordinates
        Returns: Tuple[str,str]
        """
        return self.settings.source_bucket, self._next_file_path()


class S3ParquetFileDataExtractor(S3FilesExtractor):
    def __init__(self, settings: S3SourceSettings):
        super().__init__(settings)

    def extract_next(self) -> DataFrame:
        """
        next eligible source coordinates
        Returns: Tuple[str,str]
        """
        s3 = self._get_s3_util()
        return s3.download_parquet_as_dataframe(key=self._next_file_path())


class S3SinkSettings(SinkSettings):
    target_bucket: str
    target_key_prefix: str
    connection_settings: AwsConnectionSettings


class S3Loader(Loader):
    def __init__(self, settings: S3SinkSettings):
        super().__init__(settings)
        self.settings = settings
        self._s3_util = None

    def _get_s3_util(self) -> S3Util:
        if self._s3_util is None:
            self._s3_util = S3Util(
                bucket=self.settings.target_bucket,
                conn=AwsConnectionManager(self.settings.connection_settings),
            )
        return self._s3_util

    @abstractmethod
    def load(self, data: Any) -> None:
        pass


class S3FileCopier(S3Loader):
    def __init__(self, settings: S3SinkSettings):
        super().__init__(settings)

    def _get_target_key(self, source_key: str) -> str:
        file_name = source_key.split('/')[-1]
        return f"{self.settings.target_key_prefix}/{file_name}"

    def load(self, data: Tuple[S3Bucket, S3Key]) -> None:
        """
        Transfer one source s3 key to target
        Args:
            data (Tuple[str, str]): source s3 bucket and key tuple
        Returns: None
        """
        (source_bucket, source_key) = data
        s3 = self._get_s3_util().get_client()
        copy_source = {
            'Bucket': source_bucket,
            'Key': source_key
        }
        target_key = self._get_target_key(source_key)
        LOG.info("Transferring Key s3://%s/%s to s3://%s/%s",
                 source_bucket,
                 source_key,
                 self.settings.target_bucket,
                 target_key)
        s3.copy(copy_source, self.settings.target_bucket, target_key)


class S3DataFrameAsParquetFileLoader(S3Loader):
    def __init__(self, settings: S3SinkSettings):
        super().__init__(settings)

    def load(self, data: Tuple[DataFrame, S3Key, FileName]) -> None:
        data_frame, key, file_name = data
        s3 = self._get_s3_util()
        s3.upload_dataframe_as_parquet(dataframe=data_frame, key=key, file_name=file_name)
