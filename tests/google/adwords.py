import os
import mock
from unittest import TestCase

from googleads import adwords
from googleads.adwords import ReportQueryBuilder
from py.builtin import execfile
from pytest_mock import mocker

from hip_data_tools.google.adwords import GoogleAdWordsConnectionManager, \
    GoogleAdWordsConnectionSettings, GoogleAdWordsSecretsManager, AdWordsCustomerUtil, \
    AdWordsOfflineConversionUtil, AdWordsCampaignUtil, AdWordsAdGroupUtil, AdWordsAdGroupAdUtil, \
    AdWordsReportDefinitionReader, AdWordsReportReader, AdWordsManagedCustomerUtil, \
    AdWordsParallelDataReadEstimator


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

        ad_group_util = AdWordsAdGroupUtil(conn)
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

        ad_util = AdWordsAdGroupAdUtil(conn)
        ad_util.set_query_to_fetch_all(page_size=10)
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
        ad_util.set_query_to_fetch_all(page_size=10)
        actual3 = ad_util.download_next_page_as_dict()
        expected3 = 10
        print(actual3[0])
        self.assertEqual(expected3, len(actual3))
        self.assertEqual(actual1[0], actual3[0])

    def test__should__be_able_to_estimate_splits__when__run_with_subclass(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        conn = GoogleAdWordsConnectionManager(
            GoogleAdWordsConnectionSettings(
                client_id=os.getenv("adwords_client_id"),
                user_agent="Tester",
                client_customer_id=os.getenv("adwords_client_customer_id"),
                secrets_manager=GoogleAdWordsSecretsManager()))
        print(conn)
        ad_util = AdWordsAdGroupAdUtil(conn)

    def test__should__be_able_to_get_report_fields__when__choosing_one_report_type(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        conn = GoogleAdWordsConnectionManager(
            GoogleAdWordsConnectionSettings(
                client_id=os.getenv("adwords_client_id"),
                user_agent="Tester",
                client_customer_id=os.getenv("adwords_client_customer_id"),
                secrets_manager=GoogleAdWordsSecretsManager()))
        ad_util = AdWordsReportDefinitionReader(conn=conn)
        actual = ad_util.get_report_fields("CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT")
        expected = 13
        self.assertEqual(expected, len(actual))

    def test__should__be_able_to_get_report_stream__when__choosing_one_query(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        conn = GoogleAdWordsConnectionManager(
            GoogleAdWordsConnectionSettings(
                client_id=os.getenv("adwords_client_id"),
                user_agent="Tester",
                client_customer_id=os.getenv("adwords_client_customer_id"),
                secrets_manager=GoogleAdWordsSecretsManager()))
        ad_util = AdWordsReportReader(conn=conn)
        report_query = (adwords.ReportQueryBuilder()
                        .Select('AdNetworkType1', 'Impressions', 'Clicks')
                        .From('CAMPAIGN_PERFORMANCE_REPORT')
                        .During('YESTERDAY')
                        .Build())
        actual = ad_util.awql_to_dataframe(query=report_query)
        print(actual)
        expected = (17046, 3)
        self.assertEqual(expected, actual.shape)

    def test__negative_keyword_reports(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        conn = GoogleAdWordsConnectionManager(
            GoogleAdWordsConnectionSettings(
                client_id=os.getenv("adwords_client_id"),
                user_agent="Tester",
                client_customer_id=os.getenv("adwords_client_customer_id"),
                secrets_manager=GoogleAdWordsSecretsManager()))
        ad_util = AdWordsReportReader(conn)
        report_query = (adwords.ReportQueryBuilder()
                        .Select('AccountDescriptiveName',
                                'CampaignId',
                                'CampaignName',
                                'CampaignStatus',
                                'Id',
                                'KeywordMatchType',
                                'Criteria'
                                )
                        .From('CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT')
                        .Build())
        actual = ad_util.awql_to_dataframe(query=report_query)
        print(actual)
        expected = (125493, 7)
        self.assertEqual(expected, actual.shape)

    def test__should__read_all_accounts__with__parent_id(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        conn = GoogleAdWordsConnectionManager(
            GoogleAdWordsConnectionSettings(
                client_id=os.getenv("adwords_client_id"),
                user_agent="Tester",
                client_customer_id=os.getenv("adwords_client_root_customer_id"),
                secrets_manager=GoogleAdWordsSecretsManager()))
        ad_util = AdWordsManagedCustomerUtil(conn)
        all_accounts = ad_util.get_all_accounts()
        print(all_accounts)
        expected = 58
        self.assertEqual(expected, len(all_accounts))

        actual_frame = ad_util.get_all_accounts_as_dataframe()
        print(actual_frame)
        self.assertEqual((58, 8), actual_frame.shape)

    def test__should__give_parallel_payloads__when__page_size_is_less_than_total_entries(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        conn = GoogleAdWordsConnectionManager(
            GoogleAdWordsConnectionSettings(
                client_id=os.getenv("adwords_client_id"),
                user_agent="Tester",
                client_customer_id=os.getenv("adwords_client_root_customer_id"),
                secrets_manager=GoogleAdWordsSecretsManager()))
        ad_util = AdWordsParallelDataReadEstimator(
            conn=conn, service="test_service",
            version="version_1",
            query=ReportQueryBuilder().Select(
                'CampaignId').From(
                'CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT').Build())

        def _get_total_entries(self):
            return 1000

        AdWordsParallelDataReadEstimator._get_total_entries = _get_total_entries

        expected = [{'number_of_pages': 2, 'page_size': 100, 'start_index': 0, 'worker': 0},
                    {'number_of_pages': 2, 'page_size': 100, 'start_index': 200, 'worker': 1},
                    {'number_of_pages': 2, 'page_size': 100, 'start_index': 400, 'worker': 2},
                    {'number_of_pages': 2, 'page_size': 100, 'start_index': 600, 'worker': 3},
                    {'number_of_pages': 2, 'page_size': 100, 'start_index': 800, 'worker': 4}]
        actual = AdWordsParallelDataReadEstimator.get_parallel_payloads(ad_util, page_size=100,
                                                                        number_of_workers=5)
        self.assertEqual(expected, actual)
