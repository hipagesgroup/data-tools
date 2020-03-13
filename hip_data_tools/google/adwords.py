"""
This Module handles the connection and operations on Google AdWords accounts using adwords API
"""
from typing import List

from attr import dataclass
from googleads import oauth2, AdWordsClient
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
    client_customer_id: str
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

    def get_adwords_client(self) -> AdWordsClient:
        """
        Gets the connected  adwords client, and creates one if not exist
        Returns:

        """
        if self._adwords_client is None:
            self._adwords_client = self._create_adwords_client()
        return self._adwords_client

    def _create_adwords_client(self) -> AdWordsClient:
        oauth2_client = self._get_oauth_client()
        adwords_client = AdWordsClient(
            developer_token=self.settings.secrets_manager.developer_key,
            oauth2_client=oauth2_client,
            user_agent=self.settings.user_agent)
        return adwords_client

    def _get_oauth_client(self) -> GoogleOAuth2Client:
        oauth2_client = oauth2.GoogleRefreshTokenClient(
            client_id=self.settings.client_id,
            client_secret=self.settings.secrets_manager.client_secret,
            refresh_token=self.settings.secrets_manager.refresh_token
        )
        return oauth2_client


class AdWordsUtil:
    def __init__(self, conn: GoogleAdWordsConnectionManager):
        """
        Generic Adwords Utility calss that generates the required services from the client.
        Args:
            conn (GoogleAdWordsConnectionManager): Connection manager to handle the creation of
            adwords client
        """
        self.conn = conn

    def _get_customer_service(self):
        return self.conn.get_adwords_client().GetService('CustomerService', version='v201809')


class AdWordsCustomerUtil(AdWordsUtil):

    def get_customers(self) -> List[dict]:
        """
        Gets the customer details of the adwords accounts associated with the connection
        Returns: List[dct]

        """
        return self._get_customer_service().getCustomers()
