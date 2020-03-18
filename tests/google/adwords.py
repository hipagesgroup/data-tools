import os
from unittest import TestCase

from py.builtin import execfile

from hip_data_tools.google.adwords import GoogleAdWordsConnectionManager, \
    GoogleAdWordsConnectionSettings, GoogleAdWordsSecretsManager, AdWordsCustomerUtil, \
    AdWordsOfflineConversionUtil


class TestAdWordsUtil(TestCase):

    def test_local_credentials_are_able_to_connect_to_adwords(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        adwords_util = AdWordsCustomerUtil(
            GoogleAdWordsConnectionManager(
                GoogleAdWordsConnectionSettings(
                    client_id=os.getenv("adwords_client_id"),
                    user_agent="Tester",
                    client_customer_id=None,
                    secrets_manager=GoogleAdWordsSecretsManager())))

        expected = 3
        cust = adwords_util.get_customers()
        print(cust)
        self.assertEqual(len(cust), expected)

    def test_adwords_data_upload_for_offline_conversion(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        adwords_util = AdWordsOfflineConversionUtil(
            GoogleAdWordsConnectionManager(
                GoogleAdWordsConnectionSettings(
                    client_id=os.getenv("adwords_client_id"),
                    user_agent="Tester",
                    client_customer_id=os.getenv("adwords_client_customer_id"),
                    secrets_manager=GoogleAdWordsSecretsManager())))

        uploaded, failed = adwords_util.upload_conversions(
            [
                {
                    'googleClickId': 'Cj0KCQiAqY3zBRDQARIsAJeCVxOIyZ8avQ0he3WIpHPwV6hRn'
                                     '-8Y2gDrUBJcc95tDdLcE35TK1mhhmIaAgZGEALw_wcB',
                    'conversionName': 'claim_attempts_testing',
                    'conversionTime': '20200309 074357 UTC',
                    'conversionValue': 17.0,
                    'conversionCurrencyCode': 'AUD',
                    'externalAttributionCredit': None,
                    'externalAttributionModel': None
                },
                {
                    'googleClickId': 'Cj0KCQiAqY3zBRDQARIsAJeCVxOIyZ8avQ0he3WIpHPwV6hRn'
                                     '-8Y2gDrUBJcc95tDdLcE35TK1mhhmIaAgZGEALw_wcB',
                    'conversionName': 'claim_attempts_testing',
                    'conversionTime': '20200309 074353 UTC',
                    'conversionValue': 17.0,
                    'conversionCurrencyCode': 'AUD',
                    'externalAttributionCredit': None,
                    'externalAttributionModel': None
                },
                {
                    'googleClickId': 'Cj0KCQiAqY3zBRDQARIsAJeCVxOIyZ8avQ0he3WIpHPwV6hRn'
                                     '-8Y2gDrUBJcc95tDdLcE35TK1mhhmIaAgZGEALw_wcB',
                    'conversionName': 'claim_attempts_testing',
                    'conversionTime': '20200309 023001 UTC',
                    'conversionValue': 17.0,
                    'conversionCurrencyCode': 'AUD',
                    'externalAttributionCredit': None,
                    'externalAttributionModel': None
                },
            ]
        )

        print(uploaded, failed)
        self.assertEqual(len(uploaded), 3)
        self.assertEqual(len(failed), 0)
