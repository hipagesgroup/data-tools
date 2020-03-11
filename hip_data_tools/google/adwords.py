from hip_data_tools.common import SecretsManager, KeyValueSource, ENVIRONMENT


class GoogleAdWordsSecretsManager(SecretsManager):

    def __init__(self,
                 source: KeyValueSource = ENVIRONMENT,
                 service_account_key_var="gcp_service_account_key",
                 adwords_developer_key_var="adwords_developer_key"):
        self._required_keys = [service_account_key_var, adwords_developer_key_var, ]
        super().__init__(self._required_keys, source)

        self.keyfile_object = self.get_secret(service_account_key_var)
        self.developer_key = self.get_secret(adwords_developer_key_var)


class GoogleAdWordsConnectionSettings:
    service_account_user: str
    user_agent: str
    client_customer_id: str
    secrets_manager: GoogleAdWordsSecretsManager


class GoogleAdWordsConnectionManager:
    def __init__(self, settings: GoogleAdWordsConnectionSettings):
        self.settings = settings

    def get_client(self):
