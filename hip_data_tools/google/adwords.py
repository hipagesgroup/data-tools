"""
This Module handles the connection and operations on Google AdWords accounts using adwords API
"""
import gzip
from collections import OrderedDict
from tempfile import NamedTemporaryFile
from typing import List, Optional, Any

import pandas as pd
from attr import dataclass
from googleads import oauth2, AdWordsClient
from googleads.adwords import ServiceQueryBuilder, ReportQuery
from googleads.common import GoogleSoapService
from googleads.oauth2 import GoogleOAuth2Client
from pandas import DataFrame

from hip_data_tools.common import KeyValueSource, ENVIRONMENT, SecretsManager, \
    nested_list_of_dict_to_dataframe, LOG


class GoogleAdWordsSecretsManager(SecretsManager):
    """
    Secrets manager for Google Adwords Specific secrets, these are modeled to handle the OAuth
    based google client and refresh token secrets
    Args:
        source (KeyValueSource): The Key Value source which holds secrets
        client_secret_var (str): The Key pointer for client_secret
        refresh_token_var (str): The Key pointer for refresh_token
        developer_token_var (str): The Key pointer for developer_token
    """

    def __init__(self,
                 source: KeyValueSource = ENVIRONMENT,
                 client_secret_var: str = "adwords_client_secret",
                 refresh_token_var: str = "adwords_refresh_token",
                 developer_token_var: str = "adwords_developer_token"):
        self._required_keys = [client_secret_var, refresh_token_var, developer_token_var, ]
        super().__init__(self._required_keys, source)
        self.client_secret = self.get_secret(client_secret_var)
        self.refresh_token = self.get_secret(refresh_token_var)
        self.developer_key = self.get_secret(developer_token_var)


@dataclass
class GoogleOAuthConnectionSettings:
    """
    Handle connection settings for AdWords client
    """
    client_id: str
    secrets_manager: GoogleAdWordsSecretsManager


class GoogleOAuthConnectionManager:
    """
    Creates an OAuth client from google adwords library
    Args:
        settings (GoogleOAuthConnectionSettings): connection settings to use for creation
    """

    def __init__(self, settings: GoogleOAuthConnectionSettings):
        self.__settings = settings

    def _get_oauth_client(self) -> GoogleOAuth2Client:
        """
        Create an OAuth refresh-token client for all google adwords and manager api
        Returns: GoogleOAuth2Client
        """
        return oauth2.GoogleRefreshTokenClient(
            client_id=self.__settings.client_id,
            client_secret=self.__settings.secrets_manager.client_secret,
            refresh_token=self.__settings.secrets_manager.refresh_token
        )


@dataclass
class GoogleAdWordsConnectionSettings(GoogleOAuthConnectionSettings):
    """
    Handle connection settings for AdWords client
    """
    user_agent: str
    client_customer_id: Optional[str]


class GoogleAdWordsConnectionManager(GoogleOAuthConnectionManager):
    """
    AdWords Connection Manager that manages the lifecycle of authentication and the resulting
    adwords client
    Args:
        settings (GoogleAdWordsConnectionSettings): setting to use for connecting to adwords
    """

    def __init__(self, settings: GoogleAdWordsConnectionSettings):
        super().__init__(settings)
        self.settings = settings
        self._adwords_client = None

    def get_adwords_client(self, **kwargs) -> AdWordsClient:
        """
        Gets the connected  adwords client, and creates one if not exist
        Returns (AdWordsClient): Adwords Client

        """
        if self._adwords_client is None:
            self._adwords_client = self._create_client(**kwargs)
        return self._adwords_client

    def _create_client(self, **kwargs) -> AdWordsClient:
        oauth2_client = self._get_oauth_client()
        adwords_client = AdWordsClient(
            developer_token=self.settings.secrets_manager.developer_key,
            oauth2_client=oauth2_client,
            user_agent=self.settings.user_agent,
            client_customer_id=self.settings.client_customer_id,
            **kwargs)
        return adwords_client


class AdWordsUtil:
    """
    Generic Adwords Utility class generates the required services to consume adwords API
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager, service: str, version: str):
        self.service = service
        self.version = version
        self.conn = conn
        self._service_object = None
        self._pager = None
        self.query = None

    def _get_service(self, **kwargs) -> GoogleSoapService:
        if not self._service_object:
            self._service_object = self._create_service(**kwargs)
        return self._service_object

    def _create_service(self, **kwargs) -> GoogleSoapService:
        client = self.conn.get_adwords_client(**kwargs)
        service = client.GetService(self.service, version=self.version)
        return service


class AdWordsDataReader(AdWordsUtil):
    """
    Generic Adwords Utility class generates the required services to consume adwords API
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager, service: str, version: str,
                 page_size: int = 1000):
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

    def set_query(self, query: ServiceQueryBuilder) -> None:
        """
        Sets the new query for the class to use awql, once the query is set, the download methods
        can iterate over paged results
        Args:
            query (ServiceQueryBuilder): query built as per adwords query language
        Returns: None
        """
        self.__pager = None
        self.query = query

    def _get_query(self) -> ServiceQueryBuilder:
        if self.query is None:
            raise Exception("Please set the query attribute using the method set_query(...)")
        return self.query

    def _get_query_pager(self, query: ServiceQueryBuilder):
        if self.__pager is None:
            self.__pager = query.Pager(self._get_service())
        return self.__pager

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
            conn=self.conn, service=self.service, version=self.version, query=self._get_query())
        return estimator.get_parallel_payloads(page_size, number_of_workers)


class AdWordsParallelDataReadEstimator(AdWordsUtil):
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

    def __init__(self, conn: GoogleAdWordsConnectionManager,
                 service: str,
                 version: str,
                 query: ServiceQueryBuilder):
        super().__init__(conn, service, version)
        self.query = query

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
        total_entries = self._get_total_entries()
        last_page_residue = total_entries % page_size
        number_of_pages = int(total_entries / page_size)
        if last_page_residue > 0:
            number_of_pages += 1
        pages_per_worker = round(number_of_pages / number_of_workers)
        start_index = 0
        result = []
        for worker in range(number_of_workers):
            payload = {
                "worker": worker,
                "start_index": start_index,
                "number_of_pages": pages_per_worker,
                "page_size": page_size,
            }
            start_index = start_index + (page_size * pages_per_worker)
            result.append(payload)
        return result

    def _get_total_entries(self) -> int:
        query = self.query
        result = next(query.Pager(self._get_service()))
        if 'totalNumEntries' in result:
            return result['totalNumEntries']
        return 0


class AdWordsCustomerUtil(AdWordsDataReader):
    """
    Adwords Utility to handle customer details, customer here meaning, the Adwords customer
    account, ie. the accounts that you see on top right corner on the AdWords console
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager):
        super().__init__(conn, service='CustomerService', version='v201809')

    def get_customers(self) -> List[dict]:
        """
        Gets the customer details of the adwords accounts associated with the connection
        Returns: List[dict]

        """
        return self._get_service().getCustomers()


def _find_and_append_data(fail: dict, data: List[dict]) -> dict:
    for element in fail['fieldPathElements']:
        if element['field'] == 'operations':
            fail_index = element['index']
            fail["data"] = data[fail_index]
            return fail
    return fail


class AdWordsOfflineConversionUtil(AdWordsUtil):
    """
    Adwords Utility to handle offline conversions
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager):
        super().__init__(conn, service='OfflineConversionFeedService', version='v201809')
        self.required_fields = [
            'conversionName',
            'conversionTime',
            'conversionValue',
            'googleClickId',
        ]
        self.valid_fields = self.required_fields + [
            'conversionCurrencyCode',
            'externalAttributionCredit',
            'externalAttributionModel',
        ]
        self.conversion_time_format = '%Y%m%d %H%M%S %Z'

    def upload_conversions(self, data: List[dict]) -> (List[dict], List[dict]):
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
        else:
            mutations = self._get_mutations_from_conversions_batch(data)
            result = self._upload_mutations_batch(mutations)
            if result['ListReturnValue.Type'] != 'OfflineConversionFeedReturnValue':
                raise Exception(
                    f"Unhandled Exception while loading batch of conversions, response: {result}")
            uploaded = [x for x in result['value'] if x is not None]
            #  Append actual data to the failed conversions
            fails = [_find_and_append_data(fail, data) for fail in result['partialFailureErrors']]

            return uploaded, fails

    def _upload_mutations_batch(self, mutations: List[dict]) -> dict:
        return self._get_service(partial_failure=True).mutate(mutations)

    def _get_mutations_from_conversions_batch(self, conversions: List[dict]) -> List[dict]:
        return [self._get_mutation_from_conversion(d) for d in conversions]

    def _get_mutation_from_conversion(self, conversion: dict) -> dict:
        self._verify_required_columns(conversion)
        self._verify_accepted_columns(conversion)
        return {'operator': 'ADD', 'operand': conversion}

    def _verify_required_columns(self, conversion: dict) -> None:
        for col in self.required_fields:
            if col not in conversion:
                raise ValueError(
                    f"The column {col} is required byt not present in the data {conversion}")

    def _verify_accepted_columns(self, conversion: dict) -> None:
        for col in conversion.keys():
            if col not in self.valid_fields:
                raise ValueError(
                    f"The column {col} present in the DataFrame is not in the allowed column list "
                    f"{self.valid_fields}")


def get_page_as_list_of_dict(page: dict) -> List[OrderedDict]:
    """
    Converts a list of entries from google adwords response into a list of Ordered Dictionaries
    Args:
        page (dict): the response page from google adwords api
    Returns: List[dict]
    """
    result = []
    if 'entries' in page:
        entries = page['entries']
        # These entries are a list of zeep Objects that need conversion to Dict
        result = [zeep_object_to_dict(entry) for entry in entries]
        LOG.debug("The result from the adword API: %s", result)
    else:
        LOG.info('No entries were found.')
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


class AdWordsCampaignUtil(AdWordsDataReader):
    """
    Handles the querying of Adwords Campaign service
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager):
        super().__init__(conn, service='CampaignService', version='v201809')

    def set_query_to_fetch_all(self, start_index: int = 0,
                               page_size: int = 500) -> None:
        """
        Get all campaigns associated with an account
        Returns: None
        """
        query = (ServiceQueryBuilder()
                 .Select('Id', 'Name', 'Status')
                 # .Where('Status').EqualTo('ENABLED')
                 .OrderBy('Id')
                 .Limit(start_index, page_size)
                 .Build())
        self.set_query(query)


class AdWordsAdGroupUtil(AdWordsDataReader):
    """
    Handles the querying of Adwords AdGroup service
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager):
        super().__init__(conn=conn, service='AdGroupService', version='v201809')
        self._all_query = None

    def set_query_to_fetch_by_campaign(self, campaign_id: str, start_index: int = 0,
                                       page_size: int = 500) -> None:
        """
        Get all AdGroups for the given Campaign
        Args:
            campaign_id (str): the adwords campaign to query
            page_size:
            start_index:
        Returns: None
        """
        query = (ServiceQueryBuilder()
                 .Select('Id', 'CampaignId', 'CampaignName', 'Status')
                 .Where('CampaignId').EqualTo(campaign_id)
                 .OrderBy('Id')
                 .Limit(start_index, page_size)
                 .Build())
        self.set_query(query)

    def set_query_to_fetch_all(self, start_index: int = 0, page_size: int = 500) -> None:
        """
        Get all Ad groups in the account
        Args:\
            page_size:
            start_index:
        Returns: None
        """
        query = (ServiceQueryBuilder()
                 .Select('Id', 'CampaignId', 'CampaignName', 'Status', 'Settings', 'Labels',
                         'ContentBidCriterionTypeGroup', 'BaseCampaignId', 'BaseAdGroupId',
                         'TrackingUrlTemplate', 'FinalUrlSuffix', 'UrlCustomParameters',
                         'AdGroupType')
                 .OrderBy('Id').Limit(start_index, page_size)
                 .Build())
        self.set_query(query)


class AdWordsAdGroupAdUtil(AdWordsDataReader):
    """
    Handles the querying of Adwords AdGroupAd service
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager):
        super().__init__(conn, service='AdGroupAdService', version='v201809')
        self._all_query = None

    def set_query_to_fetch_all(self, start_index: int = 0, page_size: int = 500):
        """
        Get all Ad groups Ads in the account
        Args:
            page_size:
            start_index:
        Returns: None
        """
        query = (ServiceQueryBuilder()
                 .Select('Id')
                 .OrderBy('Id')
                 .Limit(start_index, page_size)
                 .Build())
        self.set_query(query)


class AdWordsReportDefinitionReader(AdWordsUtil):
    """
    Class to interact with the ReportDefinitionService and access fields for adwords reports
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
    adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager):
        super().__init__(conn=conn, service="ReportDefinitionService", version="v201809")

    def get_report_fields(self, report_type: str) -> List[dict]:
        """
        Get a list of fields for a given report.
        Args:
            report_type (str): Possible reports are defined in the documentation here -
            https://developers.google.com/adwords/api/docs/appendix/reports/all-reports
        Returns: List[dict]
        """
        return self._get_service().getReportFields(report_type)


class AdWordsReportReader:
    """
    Generic data reader class for downloading data from report awql efficiently
    Args:
            conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager):
        self.conn = conn
        self.version = 'v201809'
        self.__downloader = None

    def _get_report_downloader(self):
        if self.__downloader is None:
            client = self.conn.get_adwords_client()
            self.__downloader = client.GetReportDownloader(version=self.version)
        return self.__downloader

    def awql_to_dataframe(self, query: ReportQuery,
                          include_zero_impressions: bool = True,
                          **kwargs) -> DataFrame:
        """
        Download the data returned by report query in a compressed format and return it in form of
        a pandas dataframe
        Args:
            query (ReportQuery): an awql report query, see example
            https://github.com/googleads/googleads-python-lib/blob/master/examples/adwords
            /v201809/reporting/stream_criteria_report_results.py#L34-L39
            include_zero_impressions (bool): include_zero_impressions
        Returns: DataFrame
        """
        with NamedTemporaryFile(mode="w+b") as temp_file:
            self._get_report_downloader().DownloadReportWithAwql(
                query,
                "GZIPPED_CSV",
                temp_file,
                skip_report_header=True,
                skip_column_header=False,
                skip_report_summary=True,
                include_zero_impressions=include_zero_impressions,
                **kwargs
            )
            with gzip.open(temp_file.name, mode="rt") as csv_file:
                dataframe = pd.read_csv(csv_file, sep=",", header=0)
        return dataframe


class AdWordsManagedCustomerUtil(AdWordsUtil):
    """
    Adwords Utility to parse through and gather Account Information for all sub accounts
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager):
        super().__init__(conn, service='ManagedCustomerService', version='v201809')
        self.selector_fields = ['Name', 'CustomerId', 'DateTimeZone', 'CurrencyCode',
                                'CanManageClients', 'TestAccount', 'AccountLabels']

    def _init_selector(self, offset, page_size):
        selector = {
            'fields': self.selector_fields,
            'paging': {
                'startIndex': str(offset),
                'numberResults': str(page_size)
            }
        }
        return selector

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
            selector['paging']['startIndex'] = str(offset)
            more_pages = offset < int(page['totalNumEntries'])

        return all_accounts

    def get_all_accounts_as_dataframe(self, page_size: int = 1000) -> DataFrame:
        """
        Gets the customer details of the adwords accounts associated with the connection
        Returns: DataFrame
        """
        return nested_list_of_dict_to_dataframe(self.get_all_accounts(page_size=page_size))
