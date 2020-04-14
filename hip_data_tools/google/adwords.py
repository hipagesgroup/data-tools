"""
This Module handles the connection and operations on Google AdWords accounts using adwords API
"""
import logging
from typing import List, Optional

from attr import dataclass
from googleads import oauth2, AdWordsClient
from googleads.adwords import ServiceQueryBuilder
from googleads.common import GoogleSoapService
from googleads.oauth2 import GoogleOAuth2Client

from hip_data_tools.common import KeyValueSource, ENVIRONMENT, SecretsManager

log = logging.getLogger(__name__)


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

    def get_oauth_client(self) -> GoogleOAuth2Client:
        """
        Create an oAuth refresh-token client for all google adwords and manager api
        Returns: GoogleOAuth2Client
        """
        return oauth2.GoogleRefreshTokenClient(
            client_id=self.client_id,
            client_secret=self.secrets_manager.client_secret,
            refresh_token=self.secrets_manager.refresh_token
        )


@dataclass
class GoogleAdWordsConnectionSettings(GoogleOAuthConnectionSettings):
    """
    Handle connection settings for AdWords client
    """
    user_agent: str
    client_customer_id: Optional[str]


class GoogleAdWordsConnectionManager:
    """
    AdWords Connection Manager that manages the lifecycle of authentication and the resulting
    adwords client
    Args:
        settings (GoogleAdWordsConnectionSettings): setting to use for connecting to adwords
    """

    def __init__(self, settings: GoogleAdWordsConnectionSettings):
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
        oauth2_client = self.settings.get_oauth_client()
        adwords_client = AdWordsClient(
            developer_token=self.settings.secrets_manager.developer_key,
            oauth2_client=oauth2_client,
            user_agent=self.settings.user_agent,
            client_customer_id=self.settings.client_customer_id,
            **kwargs)
        return adwords_client


class AdWordsUtil:
    """
    Generic Adwords Utility that generates the required services that perform actions on adwords
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager, service: str, version: str):
        self.service = service
        self.version = version
        self.conn = conn
        self.__service_object = None
        self.__pager = None
        self.query = None

    def download_all_as_dict(self) -> List[dict]:
        """
        Generates list of Dict from the adwords query language query as described in adwords api
        Returns: List[dict]
        """
        complete_result = []
        for page in self._get_query_pager(self._get_query()):
            complete_result.extend(_get_page_as_list_of_dict(page))
        return complete_result

    def download_next_page_as_dict(self) -> List[dict]:
        """
        Generates a list of dict from the next page of the API call to adwords
        Returns: List[dict]
        """
        return _get_page_as_list_of_dict(next(self._get_query_pager(self._get_query())))

    def set_query(self, query: ServiceQueryBuilder) -> None:
        """
        Sets the new query for adwords util to use awql, once the query is set, the download methods
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

    def _get_service(self, **kwargs) -> GoogleSoapService:
        if not self.__service_object:
            self.__service_object = self._create_service(**kwargs)
        return self.__service_object

    def _create_service(self, **kwargs) -> GoogleSoapService:
        return self.conn.get_adwords_client(**kwargs).GetService(self.service, version=self.version)

    def _get_query_pager(self, query: ServiceQueryBuilder):
        if self.__pager is None:
            self.__pager = query.Pager(self._get_service())
        return self.__pager


class AdWordsCustomerUtil(AdWordsUtil):
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


def _get_page_as_list_of_dict(page: dict) -> List[dict]:
    if 'entries' in page:
        return page['entries']
    else:
        log.info('No entries were found.')
        return []


class AdWordsCampaignUtil(AdWordsUtil):
    """
    Handles the querying of Adwords Campaign service
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager, page_size: int = 500):
        super().__init__(conn, service='CampaignService', version='v201809')
        self.page_size = page_size

    def set_query_to_fetch_all(self) -> None:
        """
        Get all campaigns associated with and adwords account query set on utility
        Returns: None
        """
        query = (ServiceQueryBuilder()
                 .Select('Id', 'Name', 'Status')
                 # .Where('Status').EqualTo('ENABLED')
                 .OrderBy('Id')
                 .Limit(0, self.page_size)
                 .Build())
        self.set_query(query)


class AdWordsAdGroupUtil(AdWordsUtil):
    """
    Handles the querying of Adwords AdGroup service
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager, page_size: int = 500):
        super().__init__(conn, service='AdGroupService', version='v201809')
        self.page_size = page_size
        self._all_query = None

    def set_query_to_fetch_by_campaign(self, campaign_id: str) -> None:
        """
        Get Ad groups for the given Campaign query set on utility
        Args:
            campaign_id (str): the adwords campaign to query
        Returns: None
        """
        query = (ServiceQueryBuilder()
                 .Select('Id', 'CampaignId', 'CampaignName', 'Status')
                 .Where('CampaignId').EqualTo(campaign_id)
                 .OrderBy('Id')
                 .Limit(0, self.page_size)
                 .Build())
        self.set_query(query)

    def set_query_to_fetch_all(self) -> None:
        """
        Get all Ad groups in the account query set on utility
        Returns: None
        """
        query = (ServiceQueryBuilder()
                 .Select('Id', 'CampaignId', 'CampaignName', 'Status', 'Settings', 'Labels',
                         'ContentBidCriterionTypeGroup', 'BaseCampaignId', 'BaseAdGroupId',
                         'TrackingUrlTemplate', 'FinalUrlSuffix', 'UrlCustomParameters',
                         'AdGroupType')
                 .OrderBy('Id')
                 .Limit(0, self.page_size)
                 .Build())
        self.set_query(query)


class AdWordsAdGroupAdUtil(AdWordsUtil):
    """
    Handles the querying of Adwords AdGroupAd service
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager, page_size: int = 500):
        super().__init__(conn, service='AdGroupAdService', version='v201809')
        self._all_query = None
        self.page_size = page_size

    def set_query_to_fetch_all(self):
        """
        Get all Ad groups Ads in the account query set on utility
        Returns: None
        """
        query = (ServiceQueryBuilder()
                 .Select('Id')
                 .OrderBy('Id')
                 .Limit(0, self.page_size)
                 .Build())
        self.set_query(query)
