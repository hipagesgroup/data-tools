import os
from unittest import TestCase

from py.builtin import execfile

from hip_data_tools.common import DictKeyValueSource
from hip_data_tools.google.adwords import AdWordsUtil, GoogleAdWordsConnectionManager, \
    GoogleAdWordsConnectionSettings, GoogleAdWordsSecretsManager


class TestAdWordsUtil(TestCase):

    def test_stuff_works(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        adwords_util = AdWordsUtil(
            GoogleAdWordsConnectionManager(
                GoogleAdWordsConnectionSettings(
                    client_id=os.getenv("adwords_client_id"),
                    user_agent="Tester",
                    client_customer_id="840-258-1664",
                    secrets_manager=GoogleAdWordsSecretsManager())))

        expected = 3
        self.assertEqual(len(adwords_util.get_customer_details()), expected)
