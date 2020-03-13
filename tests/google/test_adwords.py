import os
from unittest import TestCase

from py.builtin import execfile

from hip_data_tools.google.adwords import GoogleAdWordsConnectionManager, \
    GoogleAdWordsConnectionSettings, GoogleAdWordsSecretsManager, AdWordsCustomerUtil


class TestAdWordsUtil(TestCase):

    def integration_test_local_credentials_are_able_to_connect_to_adwords(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        adwords_util = AdWordsCustomerUtil(
            GoogleAdWordsConnectionManager(
                GoogleAdWordsConnectionSettings(
                    client_id=os.getenv("adwords_client_id"),
                    user_agent="Tester",
                    client_customer_id="840-258-1664",
                    secrets_manager=GoogleAdWordsSecretsManager())))

        expected = 3
        self.assertEqual(len(adwords_util.get_customers()), expected)
