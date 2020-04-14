import os
from unittest import TestCase

from py.builtin import execfile

from hip_data_tools.google.adwords import GoogleAdWordsConnectionManager, \
    GoogleAdWordsConnectionSettings, GoogleAdWordsSecretsManager, AdWordsCustomerUtil, \
    AdWordsOfflineConversionUtil, AdWordsCampaignUtil, AdWordsAdGroupUtil, AdWordsAdGroupAdUtil


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
                    'googleClickId': 'xxx',
                    'conversionName': 'claim_attempts_testing',
                    'conversionTime': '20200309 074357 UTC',
                    'conversionValue': 17.0,
                    'conversionCurrencyCode': 'AUD',
                },
                {
                    'googleClickId': 'Cj0KCQiAqY3zBRDQARIsAJeCVxOIyZ8avQ0he3WIpHPwV6hRn'
                                     '-8Y2gDrUBJcc95tDdLcE35TK1mhhmIaAgZGEALw_wcB',
                    'conversionName': 'claim_attempts_testing',
                    'conversionTime': '20200309 074353 UTC',
                    'conversionValue': 17.0,
                    'conversionCurrencyCode': 'AUD',
                },
                {
                    'googleClickId': 'Cj0KCQiAqY3zBRDQARIsAJeCVxOIyZ8avQ0he3WIpHPwV6hRn'
                                     '-8Y2gDrUBJcc95tDdLcE35TK1mhhmIaAgZGEALw_wcB',
                    'conversionName': 'claim_attempts_testing',
                    'conversionTime': '20200309 023001 UTC',
                    'conversionValue': 17.0,
                    'conversionCurrencyCode': 'AUD',
                },
            ]
        )

        print(uploaded, failed)
        self.assertEqual(len(uploaded), 2)
        self.assertEqual(len(failed), 1)

    def test__should__be_able_to_get_all_campaigns__with_one_account(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        util = AdWordsCampaignUtil(
            GoogleAdWordsConnectionManager(
                GoogleAdWordsConnectionSettings(
                    client_id=os.getenv("adwords_client_id"),
                    user_agent="Tester",
                    client_customer_id=os.getenv("adwords_client_customer_id"),
                    secrets_manager=GoogleAdWordsSecretsManager())))
        util.set_query_to_fetch_all()
        actual = util.download_all_as_dict()
        expected = 2581
        print(actual)
        self.assertEqual(expected, len(actual))

    def test__should__be_able_to_get_all_ad_groups_of_all_campaigns__with_one_account(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        conn = GoogleAdWordsConnectionManager(
            GoogleAdWordsConnectionSettings(
                client_id=os.getenv("adwords_client_id"),
                user_agent="Tester",
                client_customer_id=os.getenv("adwords_client_customer_id"),
                secrets_manager=GoogleAdWordsSecretsManager()))
        # campaign_util = AdWordsCampaignUtil(conn)
        # all_campaigns = campaign_util.get_all_campaigns()

        ad_group_util = AdWordsAdGroupUtil(conn, page_size=1000)
        ad_group_util.set_query_to_fetch_all()
        actual = ad_group_util.download_all_as_dict()
        expected = 2581
        print(actual)
        self.assertEqual(expected, len(actual))

    def test__should__be_able_to_get_all_ads__when__run_multiple_times(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        conn = GoogleAdWordsConnectionManager(
            GoogleAdWordsConnectionSettings(
                client_id=os.getenv("adwords_client_id"),
                user_agent="Tester",
                client_customer_id=os.getenv("adwords_client_customer_id"),
                secrets_manager=GoogleAdWordsSecretsManager()))

        ad_util = AdWordsAdGroupAdUtil(conn, page_size=10)
        ad_util.set_query_to_fetch_all()
        actual1 = ad_util.download_next_page_as_dict()
        expected1 = 10
        print(actual1[0])
        self.assertEqual(expected1, len(actual1))
        actual2 = ad_util.download_next_page_as_dict()
        expected2 = 10
        print(actual2[0])
        self.assertEqual(expected2, len(actual2))
        self.assertNotEqual(actual1[0], actual2[0])
        # Does a reset of page position
        ad_util.set_query_to_fetch_all()
        actual3 = ad_util.download_next_page_as_dict()
        expected3 = 10
        print(actual3[0])
        self.assertEqual(expected3, len(actual3))
        self.assertEqual(actual1[0], actual3[0])
