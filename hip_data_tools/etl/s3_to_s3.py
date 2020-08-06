"""
Module to deal with data transfer from S3 to Cassandra
"""
from typing import List

from hip_data_tools.etl.common import ETL
from hip_data_tools.etl.s3 import S3SourceSettings, S3SinkSettings, S3FileLocationExtractor, \
    S3FileCopier, S3Key


class S3ToS3FileCopy(ETL):
    """
    Class to transfer objects from s3 to s3
    Args:
        source (S3FileLocationExtractor):
        sink (S3FileCopier):
    Eg:
    >>> etl = S3ToS3FileCopy(
    ...     source = S3SourceSettings(
    ...         source_bucket="MY_SOURCE_BUCKET_NAME",
    ...         source_key_prefix="foo/bar/",
    ...         suffix="parquet",
    ...         connection_settings=aws_setting,
    ...     ),
    ...     sink = S3SinkSettings(
    ...         target_bucket="MY_TARGET_BUCKET_NAME",
    ...         target_key_prefix="bar/baz/"
    ...         connection_settings=aws_setting,
    ...     ),
    ... )
    ...
    >>>
    >>> etl.execute_next()
    ...

    Args:
        source (S3SourceSettings):
        sink (S3SinkSettings):
    """

    def __init__(self, source: S3SourceSettings, sink: S3SinkSettings):
        super().__init__(
            extractor=S3FileLocationExtractor(source),
            transformers=[],
            loader=S3FileCopier(sink)
        )

    def list_source_files(self) -> List[S3Key]:
        return self.extractor.list_source_files()
