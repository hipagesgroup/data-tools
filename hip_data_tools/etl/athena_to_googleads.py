"""
handle ETL of offline conversion data from Athena to Google Ads API
"""
from typing import List, Tuple, Dict, Any
from attr import dataclass
from cassandra.cqlengine import ValidationError
from cassandra import ConsistencyLevel
from pandas import DataFrame

from hip_data_tools.apache.cassandra import (
    CassandraConnectionSettings,
    CassandraConnectionManager,
)
from hip_data_tools.common import LOG
from hip_data_tools.etl.athena_to_dataframe import (
    AthenaToDataFrame,
    AthenaToDataFrameSettings,
)
from hip_data_tools.etl.common import (
    EtlSinkRecordStateManager, sync_etl_state_table, EtlStates
)
from hip_data_tools.google.googleads import (
    GoogleAdsOfflineConversionUtil,
    GoogleAdsConnectionManager,
    GoogleAdsConnectionSettings,
    GoogleAdsClicksConversionUtil,
    GoogleAdsUploadClickConversionsRequestUtil,
)


@dataclass
class AthenaToGoogleAdsOfflineConversionSettings(AthenaToDataFrameSettings):
    """S3 to Cassandra ETL settings"""

    transformation_column_mapping: dict
    etl_identifier: str
    etl_state_manager_keyspace: str
    etl_state_manager_connection: CassandraConnectionSettings
    destination_batch_size: int
    destination_connection_settings: GoogleAdsConnectionSettings
    conversion_action_id: str
    customer_id: str


def _get_record_signature(record: dict):
    return f"{record['gclid']}||claim_attempt||{record['conversion_date_time']}"


def _get_structured_issue(error, data):
    return {
        "error": error,
        "data": data,
    }


class AthenaToGoogleAdsOfflineConversion(AthenaToDataFrame):
    """
    Class to transfer parquet data from s3 to Cassandra
    Args:
        settings (AthenaToCassandraSettings): the settings around the etl to be executed
    """

    def __init__(self, settings: AthenaToGoogleAdsOfflineConversionSettings):
        self.__settings = settings
        super().__init__(settings)
        self._googleads = None
        self._click_conversion = None
        self._upload_click_conversion_request = None

    def upload_next(self) -> Tuple[List[dict], List[dict], List[dict]]:
        """
        Upload the next file in line from the athena table onto AdWords offline conversion
        Returns:
            verification_issues List[dict]: a tuple of lists outlining any
            verification failures
            successes List[dict]: The responses for successful uploads to
            the Google Adwords API
            failures List[dict]: The responses for failed uploads to the
            Google Adwords API
        """
        return self._process_data_frame(self.next())

    def upload_all(self) -> Tuple[List[dict], List[dict], List[dict]]:
        """
        Upload all files from the Athena table onto AdWords offline conversion
        Returns:
            verification_issues List[dict]: a tuple of lists outlining any
            verification failures
            successes List[dict]: The responses for successful uploads to
            the Google Adwords API
            failures List[dict]: The responses for failed uploads to the
            Google Adwords API
        """

        verification_issues, successes, failures = [], [], []

        for key in self.list_source_files():
            issues, success, fail = self._process_data_frame(self.get_data_frame(key))

            verification_issues.extend(issues)
            successes.extend(success)
            failures.extend(fail)

        if verification_issues:
            LOG.warning("There were %s verification failures", len(verification_issues))

            LOG.debug("All verification failures: \n %s", verification_issues)

        if failures:
            LOG.warning(
                "There were %s failures uploading to the googleads " "API", len(failures)
            )

            LOG.info("Sample Failure: \n %s", failures[0])

            LOG.debug("All failures: \n %s", failures)

        LOG.info(
            "There were %s records successfully uploaded from a total of %s submitted items",
            len(successes),
            len(successes) + len(failures) + len(verification_issues),
        )

        return verification_issues, successes, failures

    def _get_googleads_util(self):
        self._googleads = GoogleAdsOfflineConversionUtil(
            GoogleAdsConnectionManager(
                self.__settings.destination_connection_settings
            )
        )
        return self._googleads

    def _get_googleads_click_conversion_util(self):
        self._click_conversion = GoogleAdsClicksConversionUtil(
            GoogleAdsConnectionManager(
                self.__settings.destination_connection_settings
            )
        )
        return self._click_conversion

    def _get_upload_click_conversion_request_util(self):
        self._upload_click_conversion_request = GoogleAdsUploadClickConversionsRequestUtil(
            GoogleAdsConnectionManager(
                self.__settings.destination_connection_settings
            )
        )
        return self._upload_click_conversion_request

    def _get_sink_manager(self, record: dict) -> EtlSinkRecordStateManager:
        # Need to set up the cassandra connection
        return EtlSinkRecordStateManager(
            record_identifier=_get_record_signature(record),
            etl_signature=self.__settings.etl_identifier,
        )

    def _data_frame_to_destination_dict(self, data_frame: DataFrame) -> List[dict]:
        data_frame = data_frame.rename(
            columns=self.__settings.transformation_column_mapping
        )
        approved_fields = self._get_googleads_util().valid_fields
        drop_fields = [
            col for col in list(data_frame.columns) if col not in approved_fields
        ]
        data_frame = data_frame.drop(drop_fields, axis=1)
        return data_frame.to_dict("records")

    def _chunk_batches(self, lst: List[dict]) -> List[List[dict]]:
        n = self.__settings.destination_batch_size
        return [lst[i * n: (i + 1) * n] for i in range((len(lst) + n - 1) // n)]

    def _process_data_frame(self, data_frame) -> Tuple[list, list, list]:
        data_dict = self._data_frame_to_destination_dict(data_frame)
        self._state_manager_connect()
        ready_data, verification_issues = self._verify_data_before_upsert(data_dict)
        data_dict_batches = self._chunk_batches(ready_data)
        successes = []
        failures = []
        verification_issues = []

        for data_batch in data_dict_batches:
            LOG.debug(f'Data Batch: {data_batch}')
            data_to_process, processing_issue = self._mark_processing(data_batch)
            LOG.debug(f'Data to process: {data_to_process}')
            verification_issues.extend(processing_issue)
            click_conversions = [
                self._get_click_conversion_batch(dat) for dat in data_to_process
            ]
            LOG.debug(f'Click Conversions: {click_conversions}')
            request = [
                self._get_upload_conversion_request_batch(click_conversion)
                for click_conversion in click_conversions
            ]
            LOG.debug(f'Google Ads Request: {request}')
            response = [self._upload_conversions(r) for r in request]
            LOG.debug(f'Number of responses: {len(response)}')
            LOG.debug(f'API Response: {response}')
            success, fail = list(zip(*response))
            success_clean = [i for i in success if i]
            failure_clean = [i for i in fail if i]
            LOG.debug(f'Success clean records: {success_clean}')
            LOG.debug(f'Fail clean records: {failure_clean}')
            self._mark_upload_results(failure_clean, success_clean)
            successes.extend(success_clean)
            failures.extend(failure_clean)

        return verification_issues, successes, failures

    def _upload_conversions(self, data_batch):
        return self._get_googleads_util().upload_conversions(data_batch)

    def _state_manager_connect(self):

        LOG.info("Connecting to Cassandra")

        conn = CassandraConnectionManager(
            self.__settings.etl_state_manager_connection,
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        )
        conn.setup_connection(self.__settings.etl_state_manager_keyspace)

        LOG.info("Cassandra connection established")

        sync_etl_state_table()

    def _mark_processing(self, data: List[dict]) -> Tuple[List[dict], List[Dict[str, Any]]]:
        data_for_processing = []
        issues = []
        for dat in data:
            try:
                self._get_sink_manager(dat).processing()
                LOG.debug(f'Processing data: {dat}')
                data_for_processing.append(dat)
            except ValidationError as e:
                issues.append(_get_structured_issue(str(e), dat))
        return data_for_processing, issues

    def _mark_upload_results(self, fail: List[dict], success: List[dict]) -> None:
        for dat in success:
            LOG.debug(f'Uploading success data to Cassandra: {dat}')
            self._get_sink_manager(dat).succeeded()
        for dat in fail:
            LOG.debug(f'Uploading fail data to Cassandra: {dat}')
            self._get_sink_manager(dat).failed()

    def _verify_data_before_upsert(self, data: List[dict]) -> Tuple[list, list]:
        data, issues = map(list, zip(*[self._sanitise_data(dat) for dat in data]))

        # Remove None from the List
        data_clean = [i for i in data if i]
        issues_clean = [i for i in issues if i]

        if len(issues) > 0:
            LOG.warning(f"Issues found in verification, number of issues: {len(issues_clean)}", )

        return data_clean, issues_clean

    def _sanitise_data(self, dat):
        try:

            current_state = self._get_sink_manager(dat).current_state()

            LOG.debug("Current state of sink manager %s", current_state)

            if current_state == EtlStates.Ready:
                LOG.debug("Record in ready state with data: %s", dat)

                return dat, None
            else:

                LOG.debug("Sink state found to be not ready, state is %s, the "
                          "data is: "
                          "%s", current_state, dat)

                return None, _get_structured_issue(f"Current state is {current_state} "
                                                   "state", dat)
        except ValidationError as e:
            LOG.warning("Issue while trying to ready a record for the upload \n %s \n %s", e,
                        dat)
            return None, _get_structured_issue(str(e), dat)

    def _get_click_conversion_batch(self, data):
        return self._get_googleads_click_conversion_util().click_conversion(data)

    def _get_upload_conversion_request_batch(self, click_conversion):
        return self._get_upload_click_conversion_request_util().upload_click_conversion(self.__settings.customer_id,
                                                                                        click_conversion
                                                                                        )
