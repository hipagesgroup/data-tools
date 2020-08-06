"""
Module to deal with data transfer from S3 to Cassandra
"""
from typing import List, Optional

from hip_data_tools.etl.common import ETL
from hip_data_tools.etl.s3 import S3SourceSettings, S3SinkSettings, S3FileLocationExtractor, \
    S3FileCopyLoader, S3Key, S3FileNameTransformer, AddTargetS3KeyTransformer


class S3ToS3FileCopy(ETL):
    """
    Class to transfer objects from s3 to s3
    Args:
        source (S3FileLocationExtractor):
        sink (S3FileCopyLoader):
    Eg:
    >>> etl = S3ToS3FileCopy(
    ...     source = S3SourceSettings(
    ...         bucket="MY_SOURCE_BUCKET_NAME",
    ...         key_prefix="foo/bar/",
    ...         suffix="parquet",
    ...         connection_settings=aws_setting,
    ...     ),
    ...     sink = S3SinkSettings(
    ...         bucket="MY_TARGET_BUCKET_NAME",
    ...         connection_settings=aws_setting,
    ...     ),
    ...     transformers = [AddTargetS3KeyTransformer(target_key_prefix="bar/baz/")],
    ... )
    ...
    >>>
    >>> etl.execute_next()
    ...

    Args:
        source (S3SourceSettings): Settings for source s3 files
        sink (S3SinkSettings): Settings for target s3 directory
        transformers (List[S3FileNameTransformer]): Transformers to change file names
    """

    def __init__(self, source: S3SourceSettings, sink: S3SinkSettings,
                 transformers: Optional[List[S3FileNameTransformer]] = None):
        default_transformer = AddTargetS3KeyTransformer(target_key_prefix=None)
        if not transformers:
            transformers = [default_transformer]
        super().__init__(
            extractor=S3FileLocationExtractor(source),
            loader=S3FileCopyLoader(sink),
            transformers=transformers,
        )

    def list_source_files(self) -> List[S3Key]:
        """
        List the source files as per the source settings

        Returns: List[S3Key]

        """
        return self.extractor.list_source_files()
