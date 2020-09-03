from unittest import TestCase

from hip_data_tools.etl.adwords_to_athena import AdWordsReportsToAthena, \
    AdWordsReportsToAthenaSettings


class TestAdwordsReportsToAthena(TestCase):

    def test__should__return_key_prefix_with_partition_dirs__when__a_key_prefix_and_partition_values_are_given(
            self):
        adwords_reports_to_athena_util = AdWordsReportsToAthena(
            AdWordsReportsToAthenaSettings(
                source_query="",
                source_include_zero_impressions=True,
                source_connection_settings=None,
                target_bucket="test-target-bucket",
                target_key_prefix="data/external/source=adwords/type=report_archive/database=common/table=google_adwords__ad_performance_report__monthly",
                target_file_prefix="",
                target_connection_settings=None,
                transformation_field_type_mask=None,
                target_database="common",
                target_table="google_adwords__ad_performance_report__monthly",
                target_table_ddl_progress=True,
                is_partitioned_table=True,
                partition_values=[("report_start_date_dim_key", "20180901"),
                                  ("report_end_date_dim_key", "20181001")]))
        actual = adwords_reports_to_athena_util.get_target_prefix_with_partition_dirs()
        expected = "data/external/source=adwords/type=report_archive/database=common/table=google_adwords__ad_performance_report__monthly/report_start_date_dim_key=20180901/report_end_date_dim_key=20181001"
        self.assertEqual(expected, actual)

    def test__should__return_key_prefix__when__partition_values_are_empty(
            self):
        adwords_reports_to_athena_util = AdWordsReportsToAthena(
            AdWordsReportsToAthenaSettings(
                source_query="",
                source_include_zero_impressions=True,
                source_connection_settings=None,
                target_bucket="test-target-bucket",
                target_key_prefix="data/external/source=adwords/type=report_archive/database=common/table=google_adwords__ad_performance_report__monthly",
                target_file_prefix="",
                target_connection_settings=None,
                transformation_field_type_mask=None,
                target_database="common",
                target_table="google_adwords__ad_performance_report__monthly",
                target_table_ddl_progress=True,
                is_partitioned_table=False,
                partition_values=[]))
        actual = adwords_reports_to_athena_util.get_target_prefix_with_partition_dirs()
        expected = "data/external/source=adwords/type=report_archive/database=common/table=google_adwords__ad_performance_report__monthly"
        self.assertEqual(expected, actual)

    def test__should__return_key_prefix__when__partition_values_are_None(
            self):
        adwords_reports_to_athena_util = AdWordsReportsToAthena(
            AdWordsReportsToAthenaSettings(
                source_query="",
                source_include_zero_impressions=True,
                source_connection_settings=None,
                target_bucket="test-target-bucket",
                target_key_prefix="data/external/source=adwords/type=report_archive/database=common/table=google_adwords__ad_performance_report__monthly",
                target_file_prefix="",
                target_connection_settings=None,
                transformation_field_type_mask=None,
                target_database="common",
                target_table="google_adwords__ad_performance_report__monthly",
                target_table_ddl_progress=True,
                is_partitioned_table=False,
                partition_values=None))
        actual = adwords_reports_to_athena_util.get_target_prefix_with_partition_dirs()
        expected = "data/external/source=adwords/type=report_archive/database=common/table=google_adwords__ad_performance_report__monthly"
        self.assertEqual(expected, actual)
