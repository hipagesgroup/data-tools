"""
handle ETL of data from Athena to Cassandra
"""
from typing import List

from attr import dataclass
from cassandra.cqlengine import ValidationError
from pandas import DataFrame

from hip_data_tools.etl.athena_to_dataframe import AthenaToDataFrame, AthenaToDataFrameSettings
from hip_data_tools.etl.common import EtlSinkRecordStateManager
from hip_data_tools.google.adwords import AdWordsOfflineConversionUtil, \
    GoogleAdWordsConnectionManager, GoogleAdWordsConnectionSettings


@dataclass
class AthenaToAdWordsOfflineConversionSettings(AthenaToDataFrameSettings):
    """S3 to Cassandra ETL settings"""
    transformation_column_mapping: dict
    etl_identifier: str
    destination_batch_size: int
    destination_connection_settings: GoogleAdWordsConnectionSettings


class AthenaToAdWordsOfflineConversion(AthenaToDataFrame):
    """
    Class to transfer parquet data from s3 to Cassandra
    Args:
        settings (AthenaToCassandraSettings): the settings around the etl to be executed
    """

    def __init__(self, settings: AthenaToAdWordsOfflineConversionSettings):
        super().__init__(settings)
        self._adwords = None
        self.settings = settings

    def _get_adwords_util(self):
        if self._adwords is None:
            self._adwords = AdWordsOfflineConversionUtil(
                GoogleAdWordsConnectionManager(self.settings.destination_connection_settings)
            )
        return self._adwords

    def _get_record_signature(self, record: dict):
        return f"{record['googleClickId']}||||{record['conversionName']}"

    def _get_sink_manager(self, record: dict):
        return EtlSinkRecordStateManager(
            record_identifier=self._get_record_signature(record),
            etl_signature=self.settings.etl_identifier
        )

    def _data_frame_to_destination_dict(self, data_frame: DataFrame) -> List[dict]:
        data_frame = data_frame.rename(columns=self.settings.transformation_column_mapping)
        approved_fields = self._get_adwords_util().valid_fields
        drop_fields = [col for col in list(data_frame.columns) if col not in approved_fields]
        data_frame = data_frame.drop(drop_fields, axis=1)
        return data_frame.to_dict('records')

    def _chunk_batches(self, lst: List[dict]) -> List[List[dict]]:
        n = self.settings.destination_batch_size
        return [lst[i * n:(i + 1) * n] for i in range((len(lst) + n - 1) // n)]

    def upload_next(self):
        return self._process_data_frame(self.next())

    def _process_data_frame(self, data_frame):
        data_dict = self._data_frame_to_destination_dict(data_frame)
        ready_data, issues = self._verify_data_before_upsert(data_dict)
        data_dict_batches = self._chunk_batches(ready_data)
        for data_batch in data_dict_batches:
            self._mark_processing(data_batch)
            success, fail = self._get_adwords_util().upload_conversions(data_batch)
            self._mark_upload_results(fail, success)
        return issues

    def upload_all(self):
        issues = []
        for key in self.list_source_files():
            issues.append(self._process_data_frame(self.get_dataframe(key)))
        return issues

    def _mark_processing(self, data: List[dict]):
        for dat in data:
            self._get_sink_manager(dat).processing()

    def _mark_upload_results(self, fail: List[dict], success: List[dict]):
        for dat in success:
            self._get_sink_manager(dat).succeeded()
        for dat in fail:
            self._get_sink_manager(dat["data"]).failed()

    def _verify_data_before_upsert(self, data: List[dict]):
        ready = []
        issues = []
        for dat in data:
            try:
                self._get_sink_manager(dat).ready()
                ready.append(dat)
            except ValidationError as e:
                issues.append({
                    "error": str(e),
                    "data": dat,
                })
        return ready, issues
