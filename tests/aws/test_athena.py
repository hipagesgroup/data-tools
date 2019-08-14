from unittest import TestCase

import hip_data_tools.aws.athena as athena
from hip_data_tools.aws.athena import AthenaUtil


class TestAthenaUtil(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.au = AthenaUtil(database="long_lake", conn=None)

    def test__build_create_table_sql__works_for_ga_example(self):
        actual = self.au._build_create_table_sql(
            table_settings={
                "table": "abc",
                "exists": True,
                "partitions": [
                    {"column": "view", "type": "string"},
                    {"column": "start_date_key", "type": "bigint"},
                    {"column": "end_date_key", "type": "bigint"},
                ],
                "columns": [
                    {"column": "appversion", "type": "string"},
                    {"column": "date", "type": "string"},
                    {"column": "goal1completions", "type": "string"},
                    {"column": "goal1starts", "type": "string"},
                    {"column": "mobiledeviceinfo", "type": "string"},
                    {"column": "newusers", "type": "string"},
                    {"column": "operatingsystem", "type": "string"},
                    {"column": "operatingsystemversion", "type": "string"},
                    {"column": "region", "type": "string"},
                    {"column": "screenviews", "type": "string"},
                    {"column": "sessions", "type": "string"},
                    {"column": "sourcemedium", "type": "string"},
                    {"column": "timestamp", "type": "string"}
                ],
                "storage_format_selector": "parquet",
                "s3_bucket": "long_lake",
                "s3_dir": "abc",
                "encryption": False
            }
        )
        expected = """
            CREATE EXTERNAL TABLE IF NOT EXISTS abc(
              appversion string, date string, goal1completions string, goal1starts string, mobiledeviceinfo string, newusers string, operatingsystem string, operatingsystemversion string, region string, screenviews string, sessions string, sourcemedium string, timestamp string
              )

            PARTITIONED BY (
              view string, start_date_key bigint, end_date_key bigint
              )

            ROW FORMAT SERDE
              'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            STORED AS INPUTFORMAT
              'org.apache.hadoop.mapred.TextInputFormat'
            OUTPUTFORMAT
              'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION
              's3://long_lake/abc'
            TBLPROPERTIES ('has_encrypted_data'='false')
        """
        print(actual)
        self.maxDiff = None
        self.assertEquals(actual.split(), expected.split())

    def test__build_create_table_for_csv_data(self):
        actual = self.au._build_create_table_sql(
            table_settings={
                "table": "branch_reports",
                "exists": True,
                "partitions": [
                    {"column": "source", "type": "string"},
                    {"column": "report", "type": "string"},
                    {"column": "date_dim_key", "type": "string"},
                ],
                "columns": [
                    {"column": "id", "type": "string"},
                    {"column": "name", "type": "string"},
                    {"column": "timestamp", "type": "string"},
                    {"column": "timestamp_iso", "type": "string"},
                    {"column": "origin", "type": "string"},
                    {"column": "last_attributed_touch_type", "type": "string"},
                    {"column": "last_attributed_touch_timestamp", "type": "string"},
                    {"column": "last_attributed_touch_timestamp_iso", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_id", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_campaign", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_campaign_id", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_channel", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_feature", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_stage", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_tags", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_advertising_partner_name", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_secondary_publisher", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_creative_name", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_creative_id", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_ad_set_name", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_ad_set_id", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_ad_name", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_ad_id", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_branch_ad_format", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_technology_partner", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_banner_dimensions", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_placement", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_keyword_id", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_agency", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_optimization_model", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_secondary_ad_format", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_journey_name", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_journey_id", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_view_name", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_view_id", "type": "string"},
                    {"column": "last_attributed_touch_data_plus_current_feature", "type": "string"},
                    {"column": "last_attributed_touch_data_plus_via_features", "type": "string"},
                    {"column": "last_attributed_touch_data_dollar_3p", "type": "string"},
                    {"column": "last_attributed_touch_data_plus_web_format", "type": "string"},
                    {"column": "last_attributed_touch_data_custom_fields", "type": "string"},
                    {"column": "days_from_last_attributed_touch_to_event", "type": "string"},
                    {"column": "hours_from_last_attributed_touch_to_event", "type": "string"},
                    {"column": "minutes_from_last_attributed_touch_to_event", "type": "string"},
                    {"column": "seconds_from_last_attributed_touch_to_event", "type": "string"},
                    {"column": "last_cta_view_timestamp", "type": "string"},
                    {"column": "last_cta_view_timestamp_iso", "type": "string"},
                    {"column": "last_cta_view_data_tilde_id", "type": "string"},
                    {"column": "last_cta_view_data_tilde_campaign", "type": "string"},
                    {"column": "last_cta_view_data_tilde_campaign_id", "type": "string"},
                    {"column": "last_cta_view_data_tilde_channel", "type": "string"},
                    {"column": "last_cta_view_data_tilde_feature", "type": "string"},
                    {"column": "last_cta_view_data_tilde_stage", "type": "string"},
                    {"column": "last_cta_view_data_tilde_tags", "type": "string"},
                    {"column": "last_cta_view_data_tilde_advertising_partner_name", "type": "string"},
                    {"column": "last_cta_view_data_tilde_secondary_publisher", "type": "string"},
                    {"column": "last_cta_view_data_tilde_creative_name", "type": "string"},
                    {"column": "last_cta_view_data_tilde_creative_id", "type": "string"},
                    {"column": "last_cta_view_data_tilde_ad_set_name", "type": "string"},
                    {"column": "last_cta_view_data_tilde_ad_set_id", "type": "string"},
                    {"column": "last_cta_view_data_tilde_ad_name", "type": "string"},
                    {"column": "last_cta_view_data_tilde_ad_id", "type": "string"},
                    {"column": "last_cta_view_data_tilde_branch_ad_format", "type": "string"},
                    {"column": "last_cta_view_data_tilde_technology_partner", "type": "string"},
                    {"column": "last_cta_view_data_tilde_banner_dimensions", "type": "string"},
                    {"column": "last_cta_view_data_tilde_placement", "type": "string"},
                    {"column": "last_cta_view_data_tilde_keyword_id", "type": "string"},
                    {"column": "last_cta_view_data_tilde_agency", "type": "string"},
                    {"column": "last_cta_view_data_tilde_optimization_model", "type": "string"},
                    {"column": "last_cta_view_data_tilde_secondary_ad_format", "type": "string"},
                    {"column": "last_cta_view_data_plus_via_features", "type": "string"},
                    {"column": "last_cta_view_data_dollar_3p", "type": "string"},
                    {"column": "last_cta_view_data_plus_web_format", "type": "string"},
                    {"column": "last_cta_view_data_custom_fields", "type": "string"},
                    {"column": "deep_linked", "type": "string"},
                    {"column": "first_event_for_user", "type": "string"},
                    {"column": "user_data_os", "type": "string"},
                    {"column": "user_data_os_version", "type": "string"},
                    {"column": "user_data_model", "type": "string"},
                    {"column": "user_data_browser", "type": "string"},
                    {"column": "user_data_geo_country_code", "type": "string"},
                    {"column": "user_data_app_version", "type": "string"},
                    {"column": "user_data_sdk_version", "type": "string"},
                    {"column": "user_data_geo_dma_code", "type": "string"},
                    {"column": "user_data_environment", "type": "string"},
                    {"column": "user_data_platform", "type": "string"},
                    {"column": "user_data_aaid", "type": "string"},
                    {"column": "user_data_idfa", "type": "string"},
                    {"column": "user_data_idfv", "type": "string"},
                    {"column": "user_data_android_id", "type": "string"},
                    {"column": "user_data_limit_ad_tracking", "type": "string"},
                    {"column": "user_data_user_agent", "type": "string"},
                    {"column": "user_data_ip", "type": "string"},
                    {"column": "user_data_developer_identity", "type": "string"},
                    {"column": "user_data_language", "type": "string"},
                    {"column": "user_data_brand", "type": "string"},
                    {"column": "di_match_click_token", "type": "string"},
                    {"column": "event_data_revenue_in_usd", "type": "string"},
                    {"column": "event_data_exchange_rate", "type": "string"},
                    {"column": "event_data_transaction_id", "type": "string"},
                    {"column": "event_data_revenue", "type": "string"},
                    {"column": "event_data_currency", "type": "string"},
                    {"column": "event_data_shipping", "type": "string"},
                    {"column": "event_data_tax", "type": "string"},
                    {"column": "event_data_coupon", "type": "string"},
                    {"column": "event_data_affiliation", "type": "string"},
                    {"column": "event_data_search_query", "type": "string"},
                    {"column": "event_data_description", "type": "string"},
                    {"column": "custom_data", "type": "string"},
                    {"column": "last_attributed_touch_data_tilde_keyword", "type": "string"},
                    {"column": "user_data_cross_platform_id", "type": "string"},
                    {"column": "user_data_past_cross_platform_ids", "type": "string"},
                    {"column": "user_data_prob_cross_platform_ids", "type": "string"},
                    {"column": "store_install_begin_timestamp", "type": "string"},
                    {"column": "referrer_click_timestamp", "type": "string"},
                    {"column": "user_data_os_version_android", "type": "string"},
                    {"column": "user_data_geo_city_code", "type": "string"},
                    {"column": "user_data_geo_city_en", "type": "string"},
                    {"column": "user_data_http_referrer", "type": "string"},
                    {"column": "hash_version", "type": "string"}
                ],
                "storage_format_selector": "csv",
                "s3_bucket": "hipages-long-lake",
                "s3_dir": "data/external/",
                "encryption": False,
                "skip_headers": True
            }
        )

        expected = """
            CREATE EXTERNAL TABLE IF NOT EXISTS branch_reports(
               id string, name string, timestamp string, timestamp_iso string, origin string, last_attributed_touch_type string, last_attributed_touch_timestamp string, last_attributed_touch_timestamp_iso string, last_attributed_touch_data_tilde_id string, last_attributed_touch_data_tilde_campaign string, last_attributed_touch_data_tilde_campaign_id string, last_attributed_touch_data_tilde_channel string, last_attributed_touch_data_tilde_feature string, last_attributed_touch_data_tilde_stage string, last_attributed_touch_data_tilde_tags string, last_attributed_touch_data_tilde_advertising_partner_name string, last_attributed_touch_data_tilde_secondary_publisher string, last_attributed_touch_data_tilde_creative_name string, last_attributed_touch_data_tilde_creative_id string, last_attributed_touch_data_tilde_ad_set_name string, last_attributed_touch_data_tilde_ad_set_id string, last_attributed_touch_data_tilde_ad_name string, last_attributed_touch_data_tilde_ad_id string, last_attributed_touch_data_tilde_branch_ad_format string, last_attributed_touch_data_tilde_technology_partner string, last_attributed_touch_data_tilde_banner_dimensions string, last_attributed_touch_data_tilde_placement string, last_attributed_touch_data_tilde_keyword_id string, last_attributed_touch_data_tilde_agency string, last_attributed_touch_data_tilde_optimization_model string, last_attributed_touch_data_tilde_secondary_ad_format string, last_attributed_touch_data_tilde_journey_name string, last_attributed_touch_data_tilde_journey_id string, last_attributed_touch_data_tilde_view_name string, last_attributed_touch_data_tilde_view_id string, last_attributed_touch_data_plus_current_feature string, last_attributed_touch_data_plus_via_features string, last_attributed_touch_data_dollar_3p string, last_attributed_touch_data_plus_web_format string, last_attributed_touch_data_custom_fields string, days_from_last_attributed_touch_to_event string, hours_from_last_attributed_touch_to_event string, minutes_from_last_attributed_touch_to_event string, seconds_from_last_attributed_touch_to_event string, last_cta_view_timestamp string, last_cta_view_timestamp_iso string, last_cta_view_data_tilde_id string, last_cta_view_data_tilde_campaign string, last_cta_view_data_tilde_campaign_id string, last_cta_view_data_tilde_channel string, last_cta_view_data_tilde_feature string, last_cta_view_data_tilde_stage string, last_cta_view_data_tilde_tags string, last_cta_view_data_tilde_advertising_partner_name string, last_cta_view_data_tilde_secondary_publisher string, last_cta_view_data_tilde_creative_name string, last_cta_view_data_tilde_creative_id string, last_cta_view_data_tilde_ad_set_name string, last_cta_view_data_tilde_ad_set_id string, last_cta_view_data_tilde_ad_name string, last_cta_view_data_tilde_ad_id string, last_cta_view_data_tilde_branch_ad_format string, last_cta_view_data_tilde_technology_partner string, last_cta_view_data_tilde_banner_dimensions string, last_cta_view_data_tilde_placement string, last_cta_view_data_tilde_keyword_id string, last_cta_view_data_tilde_agency string, last_cta_view_data_tilde_optimization_model string, last_cta_view_data_tilde_secondary_ad_format string, last_cta_view_data_plus_via_features string, last_cta_view_data_dollar_3p string, last_cta_view_data_plus_web_format string, last_cta_view_data_custom_fields string, deep_linked string, first_event_for_user string, user_data_os string, user_data_os_version string, user_data_model string, user_data_browser string, user_data_geo_country_code string, user_data_app_version string, user_data_sdk_version string, user_data_geo_dma_code string, user_data_environment string, user_data_platform string, user_data_aaid string, user_data_idfa string, user_data_idfv string, user_data_android_id string, user_data_limit_ad_tracking string, user_data_user_agent string, user_data_ip string, user_data_developer_identity string, user_data_language string, user_data_brand string, di_match_click_token string, event_data_revenue_in_usd string, event_data_exchange_rate string, event_data_transaction_id string, event_data_revenue string, event_data_currency string, event_data_shipping string, event_data_tax string, event_data_coupon string, event_data_affiliation string, event_data_search_query string, event_data_description string, custom_data string, last_attributed_touch_data_tilde_keyword string, user_data_cross_platform_id string, user_data_past_cross_platform_ids string, user_data_prob_cross_platform_ids string, store_install_begin_timestamp string, referrer_click_timestamp string, user_data_os_version_android string, user_data_geo_city_code string, user_data_geo_city_en string, user_data_http_referrer string, hash_version string
            )
            PARTITIONED BY (
                source string, report string, date_dim_key string
            )
            ROW FORMAT SERDE 
                'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            STORED AS INPUTFORMAT
                'org.apache.hadoop.mapred.TextInputFormat'
            OUTPUTFORMAT
                'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION
                's3://hipages-long-lake/data/external/'
            TBLPROPERTIES ('has_encrypted_data'='false', 'skip.header.line.count'='1')
        """
        print(actual)
        self.maxDiff = None
        self.assertEquals(actual.split(), expected.split())

    def test__generate_parquet_ctas__creates_correct_syntax(self):
        actual = athena.generate_parquet_ctas(
            select_query="SELECT abc FROM def",
            destination_table="test",
            destination_bucket="test",
            destination_key="test")
        expected = """        
        CREATE TABLE test
        WITH (
            format='parquet',
            external_location='s3://test/test'
        ) AS
        SELECT abc FROM def
        """
        self.assertEquals(actual.split(), expected.split())

    def test__generate_csv_ctas__creates_correct_syntax(self):
        actual = athena.generate_csv_ctas(
            select_query="SELECT abc FROM def",
            destination_table="test",
            destination_bucket="test",
            destination_key="test")
        expected = """        
        CREATE TABLE test
        WITH (
            field_delimiter=',',
            format='TEXTFILE',
            external_location='s3://test/test'
        ) AS
        SELECT abc FROM def
        """
        self.assertEquals(actual.split(), expected.split())
