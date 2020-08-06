"""
S3 related ETL components Extractors, Loaders, Transformers, etc.

"""

from abc import abstractmethod
from dataclasses import dataclass
from typing import Optional, List, Tuple, Any, NewType

from pandas import DataFrame

from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.common import LOG
from hip_data_tools.etl.common import SourceSettings, Extractor, SinkSettings, Loader, Transformer

S3Key = NewType("S3Key", str)
FileName = NewType("FileName", str)
S3Bucket = NewType("S3Bucket", str)


@dataclass
class S3SourceSettings(SourceSettings):
    """ Data Class that holds source settings to connect to a s3 bucket """
    bucket: S3Bucket
    key_prefix: S3Key
    suffix: Optional[str]
    connection_settings: AwsConnectionSettings


class S3Extractor(Extractor):
    """
    Base class for all types of S3 Extractors which handles connection to s3

    Args:
        settings (S3SourceSettings):  settings to connect to the source s3 bucket
    """

    def __init__(self, settings: S3SourceSettings):
        super().__init__(settings)
        self._settings = settings
        self._s3_util = None

    def _get_s3_util(self) -> S3Util:
        if self._s3_util is None:
            self._s3_util = S3Util(
                bucket=self._settings.bucket,
                conn=AwsConnectionManager(self._settings.connection_settings),
            )
        return self._s3_util

    @abstractmethod
    def extract_next(self):
        pass

    @abstractmethod
    def has_next(self) -> bool:
        pass


class S3FilesExtractor(S3Extractor):
    """
    Base class that lists all source files and can be used downstream to extract various
    types of data from these files

    Args:
        settings (S3SourceSettings):  settings to connect to the source s3 bucket
    """

    def __init__(self, settings: S3SourceSettings):
        super().__init__(settings)
        self._source_keys = None
        self._file_counter = 0

    def list_source_files(self) -> List[S3Key]:
        """
        Lists all files that match the source settings

        Returns: List[S3Key]

        """
        if self._source_keys is None:
            keys = self._get_s3_util().get_keys(self._settings.key_prefix)
            if self._settings.suffix:
                keys = [key for key in keys if key.endswith(self._settings.suffix)]
            self._source_keys = keys
            LOG.info("Listed and cached %s source files", len(self._source_keys))
            self._file_counter = len(self._source_keys)
        return self._source_keys

    def _next_file_path(self) -> str:
        file = self.list_source_files()[self._file_counter - 1]
        self._file_counter -= 1
        return file

    @abstractmethod
    def extract_next(self) -> Tuple[str, str]:
        pass

    def has_next(self) -> bool:
        self.list_source_files()
        return self._file_counter > 0

    def reset(self) -> None:
        self._source_keys = None


class S3FileLocationExtractor(S3FilesExtractor):
    """
    Base class for S3 file name extractors

    Args:
        settings (S3SourceSettings): settings to connect to the source s3 bucket
    """

    def __init__(self, settings: S3SourceSettings):
        super().__init__(settings)

    def extract_next(self) -> Tuple[str, str]:
        """
        next eligible source coordinates
        Returns: Tuple[str,str]
        """
        return self._settings.bucket, self._next_file_path()


class S3ParquetFileDataExtractor(S3FilesExtractor):
    """
    Extractor class to read parquet files stored in s3 in form of Pandas Dataframe

    Args:
        settings (S3SourceSettings): settings to connect to the source s3 bucket
    """

    def __init__(self, settings: S3SourceSettings):
        super().__init__(settings)

    def extract_next(self) -> DataFrame:
        """
        next eligible source coordinates
        Returns: Tuple[str,str]
        """
        s3 = self._get_s3_util()
        return s3.download_parquet_as_dataframe(key=self._next_file_path())


@dataclass
class S3SinkSettings(SinkSettings):
    """ Target sink settings for S3 """
    bucket: S3Bucket
    connection_settings: AwsConnectionSettings


class S3Loader(Loader):
    """
    Base class for all S3 Loaders
    """

    def __init__(self, settings: S3SinkSettings):
        super().__init__(settings)
        self._settings = settings
        self._s3_util = None

    def _get_s3_util(self) -> S3Util:
        if self._s3_util is None:
            self._s3_util = S3Util(
                bucket=self._settings.bucket,
                conn=AwsConnectionManager(self._settings.connection_settings),
            )
        return self._s3_util

    @abstractmethod
    def load(self, data: Any) -> None:
        pass


class S3FileCopyLoader(S3Loader):
    """
    Class to copy files from various buckets to a target bucket

    Args:
        settings (S3SinkSettings): target s3 data sink settings
    """

    def __init__(self, settings: S3SinkSettings):
        super().__init__(settings)

    def load(self, data: Tuple[S3Bucket, S3Key, S3Key]) -> None:
        """
        Transfer one source s3 key to target
        Args:
            data (Tuple[S3Bucket, S3Key, S3Key]): source s3 bucket, source key and target key tuple
        Returns: None
        """
        (source_bucket, source_key, target_key) = data
        if not target_key:
            target_key = source_key
        s3 = self._get_s3_util().get_client()
        copy_source = {
            'Bucket': source_bucket,
            'Key': source_key
        }
        LOG.info("Transferring Key s3://%s/%s to s3://%s/%s",
                 source_bucket,
                 source_key,
                 self._settings.bucket,
                 target_key)
        s3.copy(copy_source, self._settings.bucket, target_key)


class S3DataFrameAsParquetFileLoader(S3Loader):
    """
    Class to Load Parquet files stored as dataframe into pandas dataframes
    Args:
        settings (S3SinkSettings): target s3 data sink settings
    """

    def __init__(self, settings: S3SinkSettings):
        super().__init__(settings)

    def load(self, data: Tuple[DataFrame, S3Key, FileName]) -> None:
        """
        Load the data that contains a dataframe, target s3 key and filename onto the sink as
        parquet files

        Args:
            data (Tuple[DataFrame, S3Key, FileName]): dataframe, target s3 key, target file name

        Returns: None

        """
        data_frame, key, file_name = data
        s3 = self._get_s3_util()
        s3.upload_dataframe_as_parquet(dataframe=data_frame, key=key, file_name=file_name)


class S3FileNameTransformer(Transformer[Tuple[S3Bucket, S3Key], Tuple[S3Bucket, S3Key]]):
    """ Abstract base class to handle Tuple[S3Bucket, S3Key] transformation """

    def __init__(self):
        pass

    @abstractmethod
    def transform(self, data: Tuple[S3Bucket, S3Key]) -> Tuple[S3Bucket, S3Key]:
        """
        Transform the data element provided

        Args:
            data (Any): A data Element

        Returns: Any

        """
        pass


class AddTargetS3KeyTransformer(S3FileNameTransformer):
    """
    Transformer to change the source prefix / directory of an s3 key into a given target
    directory, keeping file names same

    Args:
        target_key_prefix (S3Key): the new static target directory
    """

    def __init__(self, target_key_prefix: S3Key):
        super().__init__()
        self.target_key_prefix = target_key_prefix

    def transform(self, data: Tuple[S3Bucket, S3Key]) -> Tuple[S3Bucket, S3Key, S3Key]:
        """
        change the source prefix / directory of an s3 key into a given target directory,
        keeping file names same

        Args:
            data (Tuple[S3Bucket, S3Key]): Pair of source s3 bucket and key

        Returns: Tuple[S3Bucket, S3Key]

        """
        source_bucket, source_key = data
        file_name = source_key.split('/')[-1]
        target = None
        if self.target_key_prefix:
            target = S3Key(f"{self.target_key_prefix}/{file_name}")
        return source_bucket, source_key, target
