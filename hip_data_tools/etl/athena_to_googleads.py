"""
handle ETL of offline conversion data from Athena to Google Ads API
"""
from typing import List, Tuple

from attr import dataclass
from pandas import DataFrame

from hip_data_tools.apache.cassandra import (
    CassandraConnectionSettings,
)
from hip_data_tools.common import LOG
from hip_data_tools.etl.athena_to_dataframe import (
    AthenaToDataFrame,
    AthenaToDataFrameSettings,
)
from hip_data_tools.etl.common import (
    EtlSinkRecordStateManager,
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
    return f"{record['googleClickId']}||{record['conversionName']}||{record['conversionTime']}"


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

    def upload_next(self) -> (List[dict], List[dict], List[dict]):
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

    def upload_all(self) -> (List[dict], List[dict], List[dict]):
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
                "There were %s failures uploading to the adwords " "API", len(failures)
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
        if self._googleads is None:
            self._googleads = GoogleAdsOfflineConversionUtil(
                GoogleAdsConnectionManager(
                    self.__settings.destination_connection_settings
                )
            )
        return self._googleads

    def _get_googleads_click_conversion_util(self):
        if self._click_conversion is None:
            self._click_conversion = GoogleAdsClicksConversionUtil(
                GoogleAdsConnectionManager(
                    self.__settings.destination_connection_settings
                )
            )
        return self._click_conversion

    def _get_upload_click_conversion_request_util(self):
        if self._upload_click_conversion_request is None:
            self._click_conversion = GoogleAdsUploadClickConversionsRequestUtil(
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
        return [lst[i * n : (i + 1) * n] for i in range((len(lst) + n - 1) // n)]

    def _process_data_frame(self, data_frame) -> Tuple[list, list, list]:
        data_dict = self._data_frame_to_destination_dict(data_frame)
        # self._state_manager_connect()
        # ready_data, verification_issues = self._verify_data_before_upsert(data_dict)
        # data_dict_batches = self._chunk_batches(ready_data)
        data_dict_batches = self._chunk_batches(data_dict)
        successes = []
        failures = []
        verification_issues = []

        for data_batch in data_dict_batches:
            click_conversions = [
                self._get_click_conversion_batch(dat) for dat in data_batch
            ]
            request = [
                self._get_upload_conversion_request_batch(click_conversion)
                for click_conversion in click_conversions
            ]
            response = [self._upload_conversions(r) for r in request]
            success, fail = list(zip(*response))
            successes.extend(success)
            failures.extend(fail)

        return verification_issues, successes, failures

    def _upload_conversions(self, data_batch):
        return self._get_googleads_util().upload_conversions(data_batch)

    def _get_click_conversion_batch(self, data):
        return self._get_googleads_click_conversion_util().click_conversion(data)

    def _get_upload_conversion_request_batch(self, click_conversion):
        return self._get_upload_click_conversion_request_util().upload_click_conversion(
            click_conversion
        )
