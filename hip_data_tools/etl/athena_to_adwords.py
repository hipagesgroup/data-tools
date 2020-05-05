"""
handle ETL of data from Athena to Cassandra
"""
from typing import List

from attr import dataclass
from cassandra.cqlengine import ValidationError
from pandas import DataFrame

from hip_data_tools.apache.cassandra import CassandraConnectionManager, CassandraConnectionSettings
from hip_data_tools.common import LOG
from hip_data_tools.etl.athena_to_dataframe import AthenaToDataFrame, AthenaToDataFrameSettings
from hip_data_tools.etl.common import EtlSinkRecordStateManager, sync_etl_state_table, EtlStates
from hip_data_tools.google.adwords import AdWordsOfflineConversionUtil, \
    GoogleAdWordsConnectionManager, GoogleAdWordsConnectionSettings


@dataclass
class AthenaToAdWordsOfflineConversionSettings(AthenaToDataFrameSettings):
    """S3 to Cassandra ETL settings"""
    transformation_column_mapping: dict
    etl_identifier: str
    etl_state_manager_keyspace: str
    etl_state_manager_connection: CassandraConnectionSettings
    destination_batch_size: int
    destination_connection_settings: GoogleAdWordsConnectionSettings


def _get_record_signature(record: dict):
    return f"{record['googleClickId']}||{record['conversionName']}||{record['conversionTime']}"


def _get_structured_issue(error, data):
    return {
        "error": error,
        "data": data,
    }


class AthenaToAdWordsOfflineConversion(AthenaToDataFrame):
    """
    Class to transfer parquet data from s3 to Cassandra
    Args:
        settings (AthenaToCassandraSettings): the settings around the etl to be executed
    """

    def __init__(self, settings: AthenaToAdWordsOfflineConversionSettings):
        self.__settings = settings
        super().__init__(settings)
        self._adwords = None

    def upload_next(self) -> List[dict]:
        """
        Upload the next file in line from the athena table onto AdWords offline conversion
        Returns List[dict]: a list of issues in the format
        [
            {
                "error": " some error",
                "data": {
                ... original data body of the data that caused issues
                }
            },
        ]
        """
        return self._process_data_frame(self.next())

    def upload_all(self) -> List[dict]:
        """
        Upload all files from the Athena table onto AdWords offline conversion
        Returns List[dict]: a list of issues in the format
        [
            {
                "error": " some error",
                "data": {
                ... original data body of the data that caused issues
                }
            }.
        ]
        """
        issues = []
        for key in self.list_source_files():
            issues.extend(self._process_data_frame(self.get_data_frame(key)))
        return issues

    def _get_adwords_util(self):
        if self._adwords is None:
            self._adwords = AdWordsOfflineConversionUtil(
                GoogleAdWordsConnectionManager(self.__settings.destination_connection_settings)
            )
        return self._adwords

    def _get_sink_manager(self, record: dict) -> EtlSinkRecordStateManager:
        # Need to set up the cassandra connection
        return EtlSinkRecordStateManager(
            record_identifier=_get_record_signature(record),
            etl_signature=self.__settings.etl_identifier
        )

    def _data_frame_to_destination_dict(self, data_frame: DataFrame) -> List[dict]:
        data_frame = data_frame.rename(columns=self.__settings.transformation_column_mapping)
        approved_fields = self._get_adwords_util().valid_fields
        drop_fields = [col for col in list(data_frame.columns) if col not in approved_fields]
        data_frame = data_frame.drop(drop_fields, axis=1)
        return data_frame.to_dict('records')

    def _chunk_batches(self, lst: List[dict]) -> List[List[dict]]:
        n = self.__settings.destination_batch_size
        return [lst[i * n:(i + 1) * n] for i in range((len(lst) + n - 1) // n)]

    def _process_data_frame(self, data_frame) -> List[dict]:
        data_dict = self._data_frame_to_destination_dict(data_frame)
        self._state_manager_connect()
        ready_data, issues = self._verify_data_before_upsert(data_dict)
        data_dict_batches = self._chunk_batches(ready_data)
        for data_batch in data_dict_batches:
            data_to_process, processing_issue = self._mark_processing(data_batch)
            issues.extend(processing_issue)
            success, fail = self._upload_conversions(data_to_process)
            self._mark_upload_results(fail, success)
        return issues

    def _upload_conversions(self, data_batch):
        return self._get_adwords_util().upload_conversions(data_batch)

    def _state_manager_connect(self):
        conn = CassandraConnectionManager(self.__settings.etl_state_manager_connection)
        conn.setup_connection(self.__settings.etl_state_manager_keyspace)
        sync_etl_state_table()

    def _mark_processing(self, data: List[dict]) -> (List[dict], List[dict]):
        data_for_processing = []
        issues = []
        for dat in data:
            try:
                self._get_sink_manager(dat).processing()
                data_for_processing.append(dat)
            except ValidationError as e:
                issues.append(_get_structured_issue(str(e), dat))
        return data_for_processing, issues

    def _mark_upload_results(self, fail: List[dict], success: List[dict]) -> None:
        for dat in success:
            self._get_sink_manager(dat).succeeded()
        for dat in fail:
            self._get_sink_manager(dat["data"]).failed()

    def _verify_data_before_upsert(self, data: List[dict]) -> (List[dict], List[dict]):
        data, issues = map(list, zip(*[self._sanitise_data(dat) for dat in data]))
        # Remove None from the List
        return [i for i in data if i], [i for i in issues if i]

    def _sanitise_data(self, dat):
        try:
            if self._verify_state(dat):
                return dat, None
            else:
                return None, _get_structured_issue(f"Current state is not Ready", dat)
        except ValidationError as e:
            LOG.warning("Issue while trying to ready a record for the upload \n %s \n %s", e,
                        dat)
            return None, _get_structured_issue(str(e), dat)

    def _verify_state(self, data):
        current_state = self._get_sink_manager(data).current_state()
        return current_state == EtlStates.Ready
