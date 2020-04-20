import os
from unittest import TestCase

from googleads import adwords
from pandas import DataFrame
from pandas.util.testing import assert_frame_equal
from py.builtin import execfile

from hip_data_tools.google.adwords import GoogleAdWordsConnectionManager, \
    GoogleAdWordsConnectionSettings, GoogleAdWordsSecretsManager, AdWordsCustomerUtil, \
    AdWordsOfflineConversionUtil, AdWordsCampaignUtil, AdWordsAdGroupUtil, AdWordsAdGroupAdUtil, \
    AdWordsReportDefinitionReader, AdWordsReportReader


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
        ad_util = AdWordsReportDefinitionReader(conn)
        actual = ad_util.get_report_fields("CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT")
        expected = [{
            'fieldName': 'AccountCurrencyCode',
            'displayFieldName': 'Currency',
            'xmlAttributeName': 'currency',
            'fieldType': 'String',
            'fieldBehavior': 'ATTRIBUTE',
            'enumValues': [],
            'canSelect': True,
            'canFilter': True,
            'isEnumType': False,
            'isBeta': False,
            'isZeroRowCompatible': True,
            'enumValuePairs': [],
            'exclusiveFields': []
        }, {
            'fieldName': 'AccountDescriptiveName',
            'displayFieldName': 'Account',
            'xmlAttributeName': 'account',
            'fieldType': 'String',
            'fieldBehavior': 'ATTRIBUTE',
            'enumValues': [],
            'canSelect': True,
            'canFilter': True,
            'isEnumType': False,
            'isBeta': False,
            'isZeroRowCompatible': True,
            'enumValuePairs': [],
            'exclusiveFields': []
        }, {
            'fieldName': 'AccountTimeZone',
            'displayFieldName': 'Time zone',
            'xmlAttributeName': 'timeZone',
            'fieldType': 'String',
            'fieldBehavior': 'ATTRIBUTE',
            'enumValues': [],
            'canSelect': True,
            'canFilter': True,
            'isEnumType': False,
            'isBeta': False,
            'isZeroRowCompatible': True,
            'enumValuePairs': [],
            'exclusiveFields': []
        }, {
            'fieldName': 'BaseCampaignId',
            'displayFieldName': 'Base Campaign ID',
            'xmlAttributeName': 'baseCampaignID',
            'fieldType': 'Long',
            'fieldBehavior': 'ATTRIBUTE',
            'enumValues': [],
            'canSelect': True,
            'canFilter': True,
            'isEnumType': False,
            'isBeta': False,
            'isZeroRowCompatible': True,
            'enumValuePairs': [],
            'exclusiveFields': []
        }, {
            'fieldName': 'CampaignId',
            'displayFieldName': 'Campaign ID',
            'xmlAttributeName': 'campaignID',
            'fieldType': 'Long',
            'fieldBehavior': 'ATTRIBUTE',
            'enumValues': [],
            'canSelect': True,
            'canFilter': True,
            'isEnumType': False,
            'isBeta': False,
            'isZeroRowCompatible': True,
            'enumValuePairs': [],
            'exclusiveFields': []
        }, {
            'fieldName': 'CampaignName',
            'displayFieldName': 'Campaign',
            'xmlAttributeName': 'campaign',
            'fieldType': 'String',
            'fieldBehavior': 'ATTRIBUTE',
            'enumValues': [],
            'canSelect': True,
            'canFilter': True,
            'isEnumType': False,
            'isBeta': False,
            'isZeroRowCompatible': True,
            'enumValuePairs': [],
            'exclusiveFields': []
        }, {
            'fieldName': 'CampaignStatus',
            'displayFieldName': 'Campaign state',
            'xmlAttributeName': 'campaignState',
            'fieldType': 'CampaignStatus',
            'fieldBehavior': 'ATTRIBUTE',
            'enumValues': [
                'UNKNOWN',
                'ENABLED',
                'PAUSED',
                'REMOVED'
            ],
            'canSelect': True,
            'canFilter': True,
            'isEnumType': True,
            'isBeta': False,
            'isZeroRowCompatible': True,
            'enumValuePairs': [
                {
                    'enumValue': 'UNKNOWN',
                    'enumDisplayValue': 'unknown'
                },
                {
                    'enumValue': 'ENABLED',
                    'enumDisplayValue': 'enabled'
                },
                {
                    'enumValue': 'PAUSED',
                    'enumDisplayValue': 'paused'
                },
                {
                    'enumValue': 'REMOVED',
                    'enumDisplayValue': 'removed'
                }
            ],
            'exclusiveFields': []
        }, {
            'fieldName': 'Criteria',
            'displayFieldName': 'Negative keyword',
            'xmlAttributeName': 'negativeKeyword',
            'fieldType': 'String',
            'fieldBehavior': 'ATTRIBUTE',
            'enumValues': [],
            'canSelect': True,
            'canFilter': True,
            'isEnumType': False,
            'isBeta': False,
            'isZeroRowCompatible': True,
            'enumValuePairs': [],
            'exclusiveFields': []
        }, {
            'fieldName': 'CustomerDescriptiveName',
            'displayFieldName': 'Client name',
            'xmlAttributeName': 'clientName',
            'fieldType': 'String',
            'fieldBehavior': 'ATTRIBUTE',
            'enumValues': [],
            'canSelect': True,
            'canFilter': True,
            'isEnumType': False,
            'isBeta': False,
            'isZeroRowCompatible': True,
            'enumValuePairs': [],
            'exclusiveFields': []
        }, {
            'fieldName': 'ExternalCustomerId',
            'displayFieldName': 'Customer ID',
            'xmlAttributeName': 'customerID',
            'fieldType': 'Long',
            'fieldBehavior': 'ATTRIBUTE',
            'enumValues': [],
            'canSelect': True,
            'canFilter': True,
            'isEnumType': False,
            'isBeta': False,
            'isZeroRowCompatible': True,
            'enumValuePairs': [],
            'exclusiveFields': []
        }, {
            'fieldName': 'Id',
            'displayFieldName': 'Keyword ID',
            'xmlAttributeName': 'keywordID',
            'fieldType': 'Long',
            'fieldBehavior': 'ATTRIBUTE',
            'enumValues': [],
            'canSelect': True,
            'canFilter': True,
            'isEnumType': False,
            'isBeta': False,
            'isZeroRowCompatible': True,
            'enumValuePairs': [],
            'exclusiveFields': []
        }, {
            'fieldName': 'IsNegative',
            'displayFieldName': 'Is negative',
            'xmlAttributeName': 'isNegative',
            'fieldType': 'Enum',
            'fieldBehavior': 'ATTRIBUTE',
            'enumValues': [
                'TRUE',
                'FALSE'
            ],
            'canSelect': True,
            'canFilter': True,
            'isEnumType': True,
            'isBeta': False,
            'isZeroRowCompatible': True,
            'enumValuePairs': [
                {
                    'enumValue': 'TRUE',
                    'enumDisplayValue': 'true'
                },
                {
                    'enumValue': 'FALSE',
                    'enumDisplayValue': 'false'
                }
            ],
            'exclusiveFields': []
        }, {
            'fieldName': 'KeywordMatchType',
            'displayFieldName': 'Match type',
            'xmlAttributeName': 'matchType',
            'fieldType': 'KeywordMatchType',
            'fieldBehavior': 'ATTRIBUTE',
            'enumValues': [
                'EXACT',
                'PHRASE',
                'BROAD'
            ],
            'canSelect': True,
            'canFilter': True,
            'isEnumType': True,
            'isBeta': False,
            'isZeroRowCompatible': True,
            'enumValuePairs': [
                {
                    'enumValue': 'EXACT',
                    'enumDisplayValue': 'Exact'
                },
                {
                    'enumValue': 'PHRASE',
                    'enumDisplayValue': 'Phrase'
                },
                {
                    'enumValue': 'BROAD',
                    'enumDisplayValue': 'Broad'
                }
            ],
            'exclusiveFields': []
        }]
        self.assertListEqual(expected, actual)

    def test__should__be_able_to_get_report_stream__when__choosing_one_query(self):
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
                        .Select('AdNetworkType1', 'Impressions', 'Clicks')
                        .From('CAMPAIGN_PERFORMANCE_REPORT')
                        .During('YESTERDAY')
                        .Build())
        actual = ad_util.awql_to_dataframe(query=report_query)
        print(actual)
        expected = (17046, 3)
        self.assertEqual(expected, actual.shape)
