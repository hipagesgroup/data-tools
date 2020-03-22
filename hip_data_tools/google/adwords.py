"""
This Module handles the connection and operations on Google AdWords accounts using adwords API
"""
from typing import List, Optional

from attr import dataclass
from googleads import oauth2, AdWordsClient
from googleads.common import GoogleSoapService
from googleads.oauth2 import GoogleOAuth2Client

from hip_data_tools.common import KeyValueSource, ENVIRONMENT, SecretsManager


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
                 developer_token_var="adwords_developer_token"):
        self._required_keys = [client_secret_var, refresh_token_var, developer_token_var, ]
        super().__init__(self._required_keys, source)
        self.client_secret = self.get_secret(client_secret_var)
        self.refresh_token = self.get_secret(refresh_token_var)
        self.developer_key = self.get_secret(developer_token_var)


@dataclass
class GoogleAdWordsConnectionSettings:
    """
    Handle connection settings for AdWords client
    """
    client_id: str
    user_agent: str
    client_customer_id: Optional[str]
    secrets_manager: GoogleAdWordsSecretsManager


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
            self._adwords_client = self._create_adwords_client(**kwargs)
        return self._adwords_client

    def _create_adwords_client(self, **kwargs) -> AdWordsClient:
        oauth2_client = self._get_oauth_client()
        adwords_client = AdWordsClient(
            developer_token=self.settings.secrets_manager.developer_key,
            oauth2_client=oauth2_client,
            user_agent=self.settings.user_agent,
            client_customer_id=self.settings.client_customer_id,
            **kwargs)
        return adwords_client

    def _get_oauth_client(self) -> GoogleOAuth2Client:
        oauth2_client = oauth2.GoogleRefreshTokenClient(
            client_id=self.settings.client_id,
            client_secret=self.settings.secrets_manager.client_secret,
            refresh_token=self.settings.secrets_manager.refresh_token
        )
        return oauth2_client


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

    def _get_service(self, **kwargs) -> GoogleSoapService:
        return self.conn.get_adwords_client(**kwargs).GetService(self.service, version=self.version)


class AdWordsCustomerUtil(AdWordsUtil):
    """
    Adwords Utility to handle customer details, customer here meaning, the Adwords customer
    account, ie. the accounts that you see on top right corner on the AdWords console
    Args:
        conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
        adwords client
    """

    def __init__(self, conn: GoogleAdWordsConnectionManager):
        super().__init__(conn, 'CustomerService', 'v201809')

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
        super().__init__(conn, 'OfflineConversionFeedService', 'v201809')
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
