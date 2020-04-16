"""
Module to deal with data transfer from Adwords to S3
"""
from typing import List

from attr import dataclass
from googleads.adwords import ServiceQueryBuilder

from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.google.adwords import GoogleAdWordsConnectionSettings, AdWordsDataReader, \
    GoogleAdWordsConnectionManager, AdWordsParallelDataReadEstimator



@dataclass
class AdWordsToS3Settings:
    """S3 to Cassandra ETL settings"""
    source_query_fragment: ServiceQueryBuilder
    source_service: str
    source_service_version: str
    source_connection_settings: GoogleAdWordsConnectionSettings
    target_bucket: str
    target_key_prefix: str
    target_connection_settings: AwsConnectionSettings


class AdWordsToS3:
    """
    ETL Class to handle the transfer of data from adwords based on AWQL to S3 as parquet files
    Args:
        settings (AdWordsToS3Settings): the etl settings to be used
    """

    def __init__(self, settings: AdWordsToS3Settings):
        self.__settings = settings
        self._adwords_util = None
        self._s3_util = None
        self._source_keys = None
        self.start_index = None
        self.page_size = None
        self.query = None
        self.iteration_limit = 0
        self.current_iteration = 0

    def _get_s3_util(self) -> S3Util:
        if self._s3_util is None:
            self._s3_util = S3Util(
                bucket=self.__settings.target_bucket,
                conn=AwsConnectionManager(self.__settings.target_connection_settings),
            )
        return self._s3_util

    def _get_adwords_util(self) -> AdWordsDataReader:
        if self._adwords_util is None:
            self._adwords_util = AdWordsDataReader(
                conn=GoogleAdWordsConnectionManager(self.__settings.source_connection_settings),
                service=self.__settings.source_service,
                version=self.__settings.source_service_version
            )
        return self._adwords_util

    def build_query(self, start_index: int, page_size: int, num_iterations: int) -> None:
        """
        Builds the query based on the query fragment in settings, to be able to load data in
        parallel from the given start index for a number of iterations
        Args:
            start_index (int): the start index to offset the beginning of query paging
            page_size (int): number of elements in each page/api call
            num_iterations (int): total number of pages required for transfer of entire data
        Returns: None
        """
        query_fragment = self.__settings.source_query_fragment
        self.start_index = start_index
        self.page_size = page_size
        self.query = query_fragment.Limit(start_index=self.start_index, page_size=page_size).Build()
        self.iteration_limit = num_iterations
        self.current_iteration = 0

    def transfer_next_iteration(self) -> bool:
        """
        Transfers the next page of data
        Returns: bool true if the data transfer succeeded, False if reached end of iterations
        """
        if self.current_iteration < self.iteration_limit:
            au = self._get_adwords_util()
            au.set_query(self.query)
            data = au.download_next_page_as_dataframe()
            s3u = self._get_s3_util()
            s3u.upload_dataframe_as_parquet(
                dataframe=data,
                key=self.__settings.target_key_prefix,
                file_name=f"index_{self._get_current_start_index()}__"
                          f"{self._get_current_end_index()}")
            self.current_iteration += 1
            return True
        else:
            return False

    def transfer_all(self) -> None:
        """
        Iteratively transfer all pages of data
        Returns: None
        """
        while True:
            if not self.transfer_next_iteration():
                break

    def _get_current_start_index(self) -> int:
        return int(self.start_index + (self.page_size * self.current_iteration))

    def _get_current_end_index(self) -> int:
        return int(self._get_current_start_index() + (self.page_size-1))

    def get_parallel_payloads(self, page_size: int, number_of_workers: int) -> List[dict]:
        """
        gives a list of dicts that contain start index, page size, and number of iterations
        Args:
            page_size (int): number of elements in each page / api call
            number_of_workers (int): total number of parallel workers for which the payload needs
            to be distributed
        Returns: List[dict] eg:
        [
            {'number_of_pages': 393, 'page_size': 1000, 'start_index': 0, 'worker': 0},
            {'number_of_pages': 393, 'page_size': 1000, 'start_index': 393000, 'worker': 1},
            {'number_of_pages': 393, 'page_size': 1000, 'start_index': 786000, 'worker': 2},
        ]
        """

        estimator = AdWordsParallelDataReadEstimator(
            conn=GoogleAdWordsConnectionManager(self.__settings.source_connection_settings),
            service=self.__settings.source_service,
            version=self.__settings.source_service_version,
            query=self.__settings.source_query_fragment.Limit(0, 1).Build())
        return estimator.get_parallel_payloads(page_size, number_of_workers)
