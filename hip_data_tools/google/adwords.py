import json
import os
from tempfile import NamedTemporaryFile

from attr import dataclass
from googleads import oauth2, adwords

from hip_data_tools.common import KeyValueSource, ENVIRONMENT, SecretsManager


class GoogleAdWordsSecretsManager(SecretsManager):

    def __init__(self,
                 source: KeyValueSource = ENVIRONMENT,
                 key_json_var: str = "adwords_key_json",
                 adwords_developer_token_var="adwords_developer_token"):
        self._required_keys = [key_json_var, adwords_developer_token_var, ]
        super().__init__(self._required_keys, source)
        self._keyfile_path = None
        self._key_object = self.get_secret(key_json_var)
        self.developer_key = self.get_secret(adwords_developer_token_var)

    def get_keyfile_path(self):
        if self._keyfile_path is None:
            with NamedTemporaryFile(mode="w+", delete=False) as temp_file:
                json.dump(self._key_object, temp_file)
                self._keyfile_path = temp_file.name
        return self._keyfile_path

    def cleanup_keyfile(self):
        os.remove(self._keyfile_path) if self._keyfile_path else None

@dataclass
class GoogleAdWordsConnectionSettings:
    service_account_user: str
    user_agent: str
    client_customer_id: str
    secrets_manager: GoogleAdWordsSecretsManager


class GoogleAdWordsConnectionManager:
    def __init__(self, settings: GoogleAdWordsConnectionSettings):
        self.settings = settings

    def get_client(self):
        oauth2_client = oauth2.GoogleServiceAccountClient(
            key_file=self.settings.secrets_manager.get_keyfile_path(),
            scope=oauth2.GetAPIScope('adwords'),
            sub=self.settings.service_account_user,  # Check if you need this
        )
        adwords_client = adwords.AdWordsClient(
            developer_token=self.settings.secrets_manager.developer_key,
            oauth2_client=oauth2_client,
            user_agent=self.settings.user_agent,
            client_customer_id=self.settings.client_customer_id)
        self.settings.secrets_manager.cleanup_keyfile()
        return adwords_client


class AdWordsUtil():
    def __init__(self, conn: GoogleAdWordsConnectionManager):
        self.conn = conn
        self._client = None

    def _get_client(self):
        if self._client is None:
            self._client = self.conn.get_client()
        return self._client

    def get_customer_details(self):
        customer_service = self._get_client().GetService('CustomerService', version='v201702')
        customers = customer_service.getCustomers()
        print('You are logged in as a user with access to the following customers:')
        for customer in customers:
            print('\t%s' % customer['customerId'])
        return customers



