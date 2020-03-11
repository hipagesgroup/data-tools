from unittest import TestCase

from hip_data_tools.common import DictKeyValueSource
from hip_data_tools.google.adwords import AdWordsUtil, GoogleAdWordsConnectionManager, \
    GoogleAdWordsConnectionSettings, GoogleAdWordsSecretsManager


class TestS3Util(TestCase):
    @classmethod
    def setUpClass(cls):
        return

    @classmethod
    def tearDownClass(cls):
        return

    def test_stuff_works(self):
        adwords_util = AdWordsUtil(
            GoogleAdWordsConnectionManager(
                GoogleAdWordsConnectionSettings(
                    service_account_user="",
                    user_agent="Tester",
                    client_customer_id="",
                    secrets_manager=GoogleAdWordsSecretsManager(
                        source=DictKeyValueSource({
                            "key_json": {},
                            "adwords_developer_token": ""
                        }),
                        key_json_var="key_json",
                        adwords_developer_token_var="adwords_developer_token"
                    )
                )
            )
        )
        expected = None
        cust = adwords_util.get_customer_details()
        self.assertEqual(cust, expected)
