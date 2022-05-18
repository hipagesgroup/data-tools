"""
This Module handles the connection and operations on Google AdWords accounts using adwords API
"""
import math
from collections import OrderedDict
from typing import List, Optional, Any, Union
from datetime import datetime
from attr import dataclass
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads import oauth2
from pandas import DataFrame

from hip_data_tools.common import (
    KeyValueSource,
    ENVIRONMENT,
    SecretsManager,
    nested_list_of_dict_to_dataframe,
    LOG,
)


class GoogleAdsSecretsManager(SecretsManager):
    """
    Secrets manager for Google Adwords Specific secrets, these are modeled to handle the OAuth
    based google client and refresh token secrets
    Args:
        source (KeyValueSource): The Key Value source which holds secrets
        client_secret_var (str): The Key pointer for client_secret
        refresh_token_var (str): The Key pointer for refresh_token
        developer_token_var (str): The Key pointer for developer_token
    """

    def __init__(
        self,
        source: KeyValueSource = ENVIRONMENT,
        client_secret_var: str = "adwords_client_secret",
        refresh_token_var: str = "adwords_refresh_token",
        client_id_var: str = "adwords_client_id",
        client_customer_id_var: str = "adwords_client_customer_id",
        developer_token_var: str = "adwords_developer_token",
    ):
        self._required_keys = [
            client_secret_var,
            refresh_token_var,
            developer_token_var,
            client_id_var,
            client_customer_id_var,
        ]
        super().__init__(self._required_keys, source)
        self.client_secret = self.get_secret(client_secret_var)
        self.refresh_token = self.get_secret(refresh_token_var)
        self.developer_key = self.get_secret(developer_token_var)
        self.client_id = self.get_secret(client_id_var)
        self.client_customer_id = self.get_secret(client_customer_id_var)


@dataclass
class GoogleOAuthConnectionSettings:
    """
    Handle connection settings for AdWords client
    """

    client_id: str
    secrets_manager: GoogleAdsSecretsManager


class GoogleOAuthConnectionManager:
    """
    Creates an OAuth client from google ads library
    Args:
        settings (GoogleOAuthConnectionSettings): connection settings to use for creation
    """

    def __init__(self, settings: GoogleOAuthConnectionSettings):
        self.__settings = settings

    def _get_oauth_client(self) -> GoogleAdsClient:
        """
        Create an OAuth refresh-token client for all google ads and manager api
        Returns: GoogleAdsClient
        """
        return oauth2.get_installed_app_credentials(
            client_id=self.__settings.client_id,
            client_secret=self.__settings.secrets_manager.client_secret,
            refresh_token=self.__settings.secrets_manager.refresh_token,
        )


@dataclass
class GoogleAdsConnectionSettings(GoogleOAuthConnectionSettings):
    """
    Handle connection settings for AdWords client
    """

    user_agent: str
    client_customer_id: Optional[str]


class GoogleAdsConnectionManager(GoogleOAuthConnectionManager):
    """
    Google Ads Connection Manager that manages the lifecycle of authentication and the resulting
    GoogleAds client
    Args:`
        settings (GoogleAdWordsConnectionSettings): setting to use for connecting to adwords
    """

    def __init__(self, settings: GoogleAdsConnectionSettings):
        super().__init__(settings)
        self.settings = settings
        self._googleads_client = None

    def get_googleads_client(self, **kwargs) -> GoogleAdsClient:
        """
        Gets the connected google ads client, and creates one if not exist
        Returns (GoogleAdsClient): Google Ads Client

        """
        if self._googleads_client is None:
            self._googleads_client = self._create_client(**kwargs)
        return self._googleads_client

    def _create_client(self, **kwargs) -> GoogleAdsClient:
        oauth2_client = self._get_oauth_client()
        return GoogleAdsClient(
            developer_token=self.settings.secrets_manager.developer_key,
            credentials=oauth2_client,
            login_customer_id=self.settings.client_customer_id,
            **kwargs,
        )


class GoogleAdsUtil:
    """
    Generic Google Ads Utility class generates the required services to consume Google Ads API
    Args:
        conn (GoogleAdsConnectionManager): Connection manager to handle the creation of
        google ads client
    """

    def __init__(
        self,
        conn: GoogleAdsConnectionManager,
        service: str = None,
        version: str = None,
        type: str = None,
    ):
        self.service = service
        self.version = version
        self.type = type
        self.conn = conn
        self._service_object = None
        self._type_object = None
        self._pager = None
        self.query = None

    def _get_service(self, **kwargs):
        if not self._service_object:
            self._service_object = self._create_service(**kwargs)
        return self._service_object

    def _create_service(self, **kwargs):
        client = self.conn.get_googleads_client(**kwargs)
        return client.get_service(self.service, version=self.version)

    def _get_type(self, **kwargs):
        if not self._type_object:
            self._type_object = self._create_type(**kwargs)
        return self._type_object

    def _create_type(self, **kwargs):
        client = self.conn.get_googleads_client(**kwargs)
        return client.get_type(self.type, version=self.version)


class AdWordsDataReader(GoogleAdsUtil):
    """
    Generic Adwords Utility class generates the required services to consume adwords API
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(
        self,
        conn: GoogleAdsConnectionManager,
        service: str,
        version: str,
        page_size: int = 1000,
    ):
        super().__init__(conn, service, version)
        self.__pager = None
        self.query = None
        self.page_size = page_size

    def download_all_as_dict(self) -> List[dict]:
        """
        Generates list of Dict from the adwords query language query as described in adwords api
        Returns: List[dict]
        """
        complete_result = []
        for page in self._get_query_pager(self._get_query()):
            complete_result.extend(get_page_as_list_of_dict(page))
        return complete_result

    def download_all_as_dataframe(self) -> DataFrame:
        """
        Generates a data frame from the next page of the API call to adwords
        Returns: List[dict]
        """
        data_list = self.download_all_as_dict()
        return nested_list_of_dict_to_dataframe(data_list)

    def download_next_page_as_dict(self) -> List[dict]:
        """
        Generates a list of dict from the next page of the API call to adwords
        Returns: List[dict]
        """
        return get_page_as_list_of_dict(next(self._get_query_pager(self._get_query())))

    def download_next_page_as_dataframe(self) -> DataFrame:
        """
        Generates a data frame from the next page of the API call to adwords
        Returns: List[dict]
        """
        data_list = self.download_next_page_as_dict()
        return nested_list_of_dict_to_dataframe(data_list)

    def set_query(self, query) -> None:
        """
        Sets the new query for the class to use awql, once the query is set, the download methods
        can iterate over paged results
        Args:
            query (ServiceQueryBuilder): query built as per adwords query language
        Returns: None
        """
        self.__pager = None
        self.query = query

    def _get_query(self):
        if self.query is None:
            raise Exception(
                "Please set the query attribute using the method set_query(...)"
            )
        return self.query

    def _get_query_pager(self, query):
        if self.__pager is None:
            self.__pager = query.Pager(self._get_service())
        return self.__pager

    def get_parallel_payloads(
        self, page_size: int, number_of_workers: int
    ) -> List[dict]:
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
            conn=self.conn,
            service=self.service,
            version=self.version,
            query=self._get_query(),
        )
        return estimator.get_parallel_payloads(page_size, number_of_workers)


def _refine_result_list(
    number_of_workers: int, page_size: int, result: list, total_entries: int
) -> None:
    last_page_size = total_entries % page_size
    number_of_workers_with_non_zero_pages = math.ceil(total_entries / page_size)
    for workers_with_non_zero_pages in range(number_of_workers_with_non_zero_pages):
        result[workers_with_non_zero_pages]["number_of_pages"] = 1
        result[workers_with_non_zero_pages]["page_size"] = page_size
    for worker_with_zero_pages in range(
        number_of_workers_with_non_zero_pages, number_of_workers
    ):
        result[worker_with_zero_pages]["start_index"] = 0
    result[number_of_workers_with_non_zero_pages - 1]["page_size"] = last_page_size


def _refine_page_confings(
    page_size: int, pages_per_worker: int, total_entries: int, number_of_workers: int
) -> Union[int, int]:
    page_size_per_worker = page_size
    refined_number_of_pages_per_worker = pages_per_worker
    if page_size >= total_entries:
        page_size_per_worker = 0
    elif page_size * number_of_workers > total_entries:
        page_size_per_worker = 0
        refined_number_of_pages_per_worker = 0
    return page_size_per_worker, refined_number_of_pages_per_worker


class AdWordsParallelDataReadEstimator(GoogleAdsUtil):
    """
    Class to use a service endpoint and awql query to estimate the number of parallel payloads, and
    their offsets
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
        service (str): Adwords service api to use for querying
        version (str): Adwords service api version to use for querying
        query (ServiceQueryBuilder): the Query builder object without limit clause
    """

    def __init__(
        self, conn: GoogleAdsConnectionManager, service: str, version: str, query
    ):
        super().__init__(conn, service, version)
        self.query = query

    def get_parallel_payloads(
        self, page_size: int, number_of_workers: int
    ) -> List[dict]:
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
        total_entries = self._get_total_entries()
        last_page_residue = total_entries % page_size
        number_of_pages = int(total_entries / page_size)
        if last_page_residue > 0:
            number_of_pages += 1
        pages_per_worker = round(number_of_pages / number_of_workers)
        start_index = 0
        result = []
        (
            page_size_per_worker,
            refined_number_of_pages_per_worker,
        ) = _refine_page_confings(
            page_size, pages_per_worker, total_entries, number_of_workers
        )
        for worker in range(number_of_workers):
            payload = {
                "worker": worker,
                "start_index": start_index,
                "number_of_pages": refined_number_of_pages_per_worker,
                "page_size": page_size_per_worker,
            }
            start_index += page_size * pages_per_worker
            result.append(payload)
        if page_size >= total_entries:
            result[0]["number_of_pages"] = 1
            result[0]["page_size"] = total_entries
        elif page_size * number_of_workers > total_entries:
            _refine_result_list(number_of_workers, page_size, result, total_entries)
        return result

    def _get_total_entries(self) -> int:
        query = self.query
        result = next(query.Pager(self._get_service()))
        if "totalNumEntries" in result:
            return result["totalNumEntries"]
        return 0


class GoogleAdsCustomerUtil(AdWordsDataReader):
    """
    Google ads Utility to handle customer details, customer here meaning, the Google ads customer
    account, ie. the accounts that you see on top right corner on the Google ads console
    Args:
        conn (GoogleAdsConnectionManager): Connection manager to handle the creation of
        Google ads client
    """

    def __init__(self, conn: GoogleAdsConnectionManager):
        super().__init__(conn, service="CustomerService", version="v10")

    def get_customers(self) -> List[dict]:
        """
        Gets the customer details of the adwords accounts associated with the connection
        Returns: List[dict]

        """
        return self._get_service().list_accessible_customers()


def _find_and_append_data(fail: dict, data: List[dict]) -> dict:
    for element in fail["fieldPathElements"]:
        if element["field"] == "operations":
            fail_index = element["index"]
            fail["data"] = data[fail_index]
            return fail
    return fail


class GoogleAdsClicksConversionUtil(GoogleAdsUtil):
    """
    Google ads Utility to handle clicks conversion
    Args:
        conn (GoogleAdsConnectionManager): Connection manager to handle the creation of
        Google ads client

    """

    def __init__(self, conn: GoogleAdsConnectionManager):
        super().__init__(conn, type="ClickConversion", version="v10")

    def click_conversion(self, data):
        """
        Gets the customer details of the adwords accounts associated with the connection
        Returns: List[dict]

        """
        click_conversion = self._get_type()
        click_conversion.gclid = data["gclid"]
        click_conversion.conversion_value = data["conversion_value"]
        click_conversion.conversion_date_time = datetime.strptime(
            data["conversion_date_time"], "%Y%m%d %H%M%S UTC"
        ).strftime("%Y-%m-%d %H:%M:%S-09:00")
        click_conversion.conversion_action = data["conversion_action"]
        click_conversion.currency_code = data["currency_code"]
        return click_conversion


class GoogleAdsUploadClickConversionsRequestUtil(GoogleAdsUtil):
    """
    Google ads Utility to upload click conversions
    Args:
        conn (GoogleAdsConnectionManager): Connection manager to handle the creation of
        Google ads client

    """

    def __init__(self, conn: GoogleAdsConnectionManager):
        super().__init__(conn, type="UploadClickConversionsRequest", version="v10")

    def upload_click_conversion(self, customer_id: str, click_conversion):
        """
        Gets the customer details of the adwords accounts associated with the connection
        Returns: List[dict]

        """
        request = self._get_type()
        request.customer_id = customer_id
        request.conversions.append(click_conversion)
        request.partial_failure = True
        return request


class GoogleAdsConversionActionUtil(GoogleAdsUtil):
    """
     Google ads Utility to get conversion action
    Args:
        conn (GoogleAdsConnectionManager): Connection manager to handle the creation of
        Google ads client

    """

    def __init__(self, conn: GoogleAdsConnectionManager):
        super().__init__(conn, service="ConversionActionService", version="v10")

    def get_conversion_action(self, customer_id: str, conversion_action_id: str):
        """
        Gets the conversion action of the googlea ds accounts associated with the connection
        Returns: List[dict]

        """
        return self._get_service().conversion_action_path(
            customer_id, conversion_action_id
        )


class GoogleAdsOfflineConversionUtil(GoogleAdsUtil):
    """
    Google Ads Utility to handle offline conversions
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdsConnectionManager):
        super().__init__(conn, service="ConversionUploadService", version="v10")
        self.required_fields = [
            "gclid",
            "conversion_action",
            "conversion_date_time",
            "conversion_value",
        ]
        self.valid_fields = self.required_fields + [
            "currency_code",
            "externalAttributionCredit",
            "externalAttributionModel",
        ]
        self.conversion_time_format = "%Y%m%d %H%M%S %Z"

    def test_upload_conversion(self):
        """
        Gets the customer details of the adwords accounts associated with the connection
        Returns: List[dict]

        """
        return self._get_service()

    def upload_conversions(self, data):
        """
        Upload a list of conversions as a batch
        Args:
            data List[dict]: a List of conversion dictionaries like:
            [
                {
                    'googleClickId': 'some valid gclid',
                    'conversionName': 'some conversion name',
                    'conversionTime': '20200309 074353 UTC',
                    'conversionValue': 17.0,
                    'conversionCurrencyCode': 'AUD',
                },
                {
                    'googleClickId': 'some other valid gclid',
                    'conversionName': 'some conversion name',
                    'conversionTime': '20200309 023001 UTC',
                    'conversionValue': 17.0,
                    'conversionCurrencyCode': 'AUD',
                    'externalAttributionCredit': 0.3,
                    'externalAttributionModel': 'Linear',
                },
            ]

        Returns (List[dict], List[dict]): A tuple of lists holding succeeded and failed records,
        respectively eg.
        (
            [
                {
                    'googleClickId': 'gclid that succeeded',
                    'conversionName': 'some conversions name',
                    'conversionTime': '20200309 074353 UTC',
                    'conversionValue': 17.0,
                    'conversionCurrencyCode': 'AUD',
                    'externalAttributionCredit': None,
                    'externalAttributionModel': None
                },
            ] ,
             [
                 {
                    'fieldPath': 'operations[0].operand',
                    'fieldPathElements': [
                        {
                            'field': 'operations',
                            'index': 0
                        },
                        {
                            'field': 'operand',
                            'index': None
                        }
                    ],
                    'trigger': None,
                    'errorString': 'OfflineConversionError.UNPARSEABLE_GCLID',
                    'ApiError.Type': 'OfflineConversionError',
                    'reason': 'UNPARSEABLE_GCLID',
                    'data': {
                        'googleClickId': 'Gclid that failed',
                        'conversionName': 'some conversion name',
                        'conversionTime': '20200309 074353 UTC',
                        'conversionValue': 17.0,
                        'conversionCurrencyCode': 'AUD',
                        'externalAttributionCredit': None,
                        'externalAttributionModel': None
                    }
                },
            ]
        )

        """
        if not data:
            return [], []
        result = self._upload_mutations_batch(data)
        return self._get_response(data, result)

    def _upload_mutations_batch(self, mutations):
        return self._get_service().upload_click_conversions(mutations)

    def _get_conversions(self, data):
        conversions = getattr(data, "conversions")[0]
        return {
            "gclid": conversions.gclid,
            "conversion_name": "conversion_action",
            "conversion_date_time": datetime.strptime(
                conversions.conversion_date_time, "%Y-%m-%d %H:%M:%S%z"
            ).strftime("%Y%m%d %H%M%S UTC"),
        }

    def _get_response(self, data, result):
        uploaded, partial_failure = None, None
        if getattr(result, "partial_failure_error").code == 0:
            uploaded = self._get_conversions(data)
        else:
            partial_failure = self._get_conversions(data)
        return uploaded, partial_failure


def get_page_as_list_of_dict(page: dict) -> List[OrderedDict]:
    """
    Converts a list of entries from google adwords response into a list of Ordered Dictionaries
    Args:
        page (dict): the response page from google adwords api
    Returns: List[dict]
    """
    result = []
    if "entries" in page:
        entries = page["entries"]
        # These entries are a list of zeep Objects that need conversion to Dict
        result = [zeep_object_to_dict(entry) for entry in entries]
        LOG.debug("The result from the adword API: %s", result)
    else:
        LOG.info("No entries were found.")
    return result


def zeep_object_to_dict(obj: Any) -> OrderedDict:
    """
    converts the zeep client objects used in google adwords to Ordered Dict, also converts nested
    objects to ordered dict recursively
    Args:
        obj (Any): the Zeep object to be converted
    Returns: OrderedDict
    """
    obj_dict = obj.__dict__["__values__"]
    for key, val in obj_dict.items():
        if "zeep.objects" in f"{type(val)}":
            obj_dict[key] = zeep_object_to_dict(val)
    return obj_dict


class AdWordsManagedCustomerUtil(GoogleAdsUtil):
    """
    Adwords Utility to parse through and gather Account Information for all sub accounts
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdsConnectionManager):
        super().__init__(conn, service="ManagedCustomerService", version="v201809")
        self.selector_fields = [
            "Name",
            "CustomerId",
            "DateTimeZone",
            "CurrencyCode",
            "CanManageClients",
            "TestAccount",
            "AccountLabels",
        ]

    def _init_selector(self, offset, page_size):
        return {
            "fields": self.selector_fields,
            "paging": {"startIndex": str(offset), "numberResults": str(page_size)},
        }

    def get_all_accounts(self, page_size: int = 1000) -> List[dict]:
        """
        Gets the customer details of the adwords accounts associated with the connection
        Returns: List[dict]
        """
        # Construct selector to get all accounts.
        offset = 0
        selector = self._init_selector(offset, page_size)
        more_pages = True
        all_accounts = []
        while more_pages:
            # Get serviced account graph.
            page = self._get_service().get(selector)
            all_accounts.extend(get_page_as_list_of_dict(page))
            offset += page_size
            selector["paging"]["startIndex"] = str(offset)
            more_pages = offset < int(page["totalNumEntries"])

        return all_accounts

    def get_all_accounts_as_dataframe(self, page_size: int = 1000) -> DataFrame:
        """
        Gets the customer details of the adwords accounts associated with the connection
        Returns: DataFrame
        """
        return nested_list_of_dict_to_dataframe(
            self.get_all_accounts(page_size=page_size)
        )
