from unittest import TestCase

from googleads.adwords import ReportQueryBuilder

from hip_data_tools.google.adwords import GoogleAdWordsConnectionManager, \
    GoogleAdWordsConnectionSettings, AdWordsParallelDataReadEstimator


class TestAdWordsParallelDataReadEstimator(TestCase):

    @classmethod
    def setUpClass(cls):
        conn = GoogleAdWordsConnectionManager(
            GoogleAdWordsConnectionSettings(
                client_id="adwords_client_id",
                user_agent="Tester",
                client_customer_id="adwords_client_root_customer_id",
                secrets_manager=None))
        cls.ad_util = AdWordsParallelDataReadEstimator(
            conn=conn, service="test_service",
            version="version_1",
            query=ReportQueryBuilder().Select(
                'CampaignId').From(
                'CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT').Build())

    def test__should__give_parallel_payloads__when__page_size_is_less_than_total_entries(self):
        def _get_total_entries(self):
            return 1000

        AdWordsParallelDataReadEstimator._get_total_entries = _get_total_entries

        expected = [{'number_of_pages': 2, 'page_size': 100, 'start_index': 0, 'worker': 0},
                    {'number_of_pages': 2, 'page_size': 100, 'start_index': 200, 'worker': 1},
                    {'number_of_pages': 2, 'page_size': 100, 'start_index': 400, 'worker': 2},
                    {'number_of_pages': 2, 'page_size': 100, 'start_index': 600, 'worker': 3},
                    {'number_of_pages': 2, 'page_size': 100, 'start_index': 800, 'worker': 4}]
        actual = AdWordsParallelDataReadEstimator.get_parallel_payloads(self.ad_util, page_size=100,
                                                                        number_of_workers=5)
        self.assertEqual(expected, actual)

    def test__should__give_parallel_payloads__when__page_size_is_greater_than_total_entries(self):
        def _get_total_entries(self):
            return 20

        AdWordsParallelDataReadEstimator._get_total_entries = _get_total_entries

        expected = [{'number_of_pages': 1, 'page_size': 20, 'start_index': 0, 'worker': 0},
                    {'number_of_pages': 0, 'page_size': 0, 'start_index': 0, 'worker': 1},
                    {'number_of_pages': 0, 'page_size': 0, 'start_index': 0, 'worker': 2},
                    {'number_of_pages': 0, 'page_size': 0, 'start_index': 0, 'worker': 3},
                    {'number_of_pages': 0, 'page_size': 0, 'start_index': 0, 'worker': 4}]
        actual = AdWordsParallelDataReadEstimator.get_parallel_payloads(self.ad_util, page_size=100,
                                                                        number_of_workers=5)
        self.assertEqual(expected, actual)

    def test__should__give_parallel_payloads__when__page_size_is_equal_to_total_entries(self):
        def _get_total_entries(self):
            return 100

        AdWordsParallelDataReadEstimator._get_total_entries = _get_total_entries

        expected = [{'number_of_pages': 1, 'page_size': 100, 'start_index': 0, 'worker': 0},
                    {'number_of_pages': 0, 'page_size': 0, 'start_index': 0, 'worker': 1},
                    {'number_of_pages': 0, 'page_size': 0, 'start_index': 0, 'worker': 2},
                    {'number_of_pages': 0, 'page_size': 0, 'start_index': 0, 'worker': 3},
                    {'number_of_pages': 0, 'page_size': 0, 'start_index': 0, 'worker': 4}]
        actual = AdWordsParallelDataReadEstimator.get_parallel_payloads(self.ad_util, page_size=100,
                                                                        number_of_workers=5)
        self.assertEqual(expected, actual)

    def test__should__give_parallel_payloads__when__page_size_times_number_of_workers_is_greater_than_total_entries(
            self):
        def _get_total_entries(self):
            return 250

        AdWordsParallelDataReadEstimator._get_total_entries = _get_total_entries

        expected = [{'number_of_pages': 1, 'page_size': 100, 'start_index': 0, 'worker': 0},
                    {'number_of_pages': 1, 'page_size': 100, 'start_index': 100, 'worker': 1},
                    {'number_of_pages': 1, 'page_size': 50, 'start_index': 200, 'worker': 2},
                    {'number_of_pages': 0, 'page_size': 0, 'start_index': 0, 'worker': 3},
                    {'number_of_pages': 0, 'page_size': 0, 'start_index': 0, 'worker': 4}]
        actual = AdWordsParallelDataReadEstimator.get_parallel_payloads(self.ad_util, page_size=100,
                                                                        number_of_workers=5)
        self.assertEqual(expected, actual)

    def test__should__give_parallel_payloads__when__page_size_times_number_of_workers_is_equal_to_total_entries(
            self):
        def _get_total_entries(self):
            return 500

        AdWordsParallelDataReadEstimator._get_total_entries = _get_total_entries

        expected = [{'number_of_pages': 1, 'page_size': 100, 'start_index': 0, 'worker': 0},
                    {'number_of_pages': 1, 'page_size': 100, 'start_index': 100, 'worker': 1},
                    {'number_of_pages': 1, 'page_size': 100, 'start_index': 200, 'worker': 2},
                    {'number_of_pages': 1, 'page_size': 100, 'start_index': 300, 'worker': 3},
                    {'number_of_pages': 1, 'page_size': 100, 'start_index': 400, 'worker': 4}]
        actual = AdWordsParallelDataReadEstimator.get_parallel_payloads(self.ad_util, page_size=100,
                                                                        number_of_workers=5)
        self.assertEqual(expected, actual)
