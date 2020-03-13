from attr import dataclass
from googleads import oauth2, adwords

from hip_data_tools.common import KeyValueSource, ENVIRONMENT, SecretsManager


class GoogleAdWordsSecretsManager(SecretsManager):

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
    client_id: str
    user_agent: str
    client_customer_id: str
    secrets_manager: GoogleAdWordsSecretsManager


class GoogleAdWordsConnectionManager:
    def __init__(self, settings: GoogleAdWordsConnectionSettings):
        self.settings = settings

    def get_adwords_client(self):
        oauth2_client = self.get_oauth_client()
        adwords_client = adwords.AdWordsClient(
            developer_token=self.settings.secrets_manager.developer_key,
            oauth2_client=oauth2_client,
            user_agent=self.settings.user_agent)
        return adwords_client

    def get_oauth_client(self):
        oauth2_client = oauth2.GoogleRefreshTokenClient(
            client_id=self.settings.client_id,
            client_secret=self.settings.secrets_manager.client_secret,
            refresh_token=self.settings.secrets_manager.refresh_token
        )
        return oauth2_client


class AdWordsUtil():
    def __init__(self, conn: GoogleAdWordsConnectionManager):
        self.conn = conn
        self._client = None

    def _get_client(self):
        if self._client is None:
            self._client = self.conn.get_adwords_client()
        return self._client

    def get_customer_details(self):
        client = self._get_client()
        customer_service = client.GetService('CustomerService', version='v201809')
        customers = customer_service.getCustomers()
        return customers
