import os
from unittest import TestCase
from unittest.mock import Mock, MagicMock
from cassandra.policies import DCAwareRoundRobinPolicy
from pandas import DataFrame
from py.builtin import execfile
from retrying import retry
from testcontainers.compose import DockerCompose
import tests.test_common as base
from hip_data_tools.apache.cassandra import CassandraConnectionSettings, CassandraSecretsManager, \
    CassandraConnectionManager
from hip_data_tools.aws.common import AwsConnectionSettings
from hip_data_tools.common import DictKeyValueSource
from hip_data_tools.etl.athena_to_adwords import AthenaToAdWordsOfflineConversion, \
    AthenaToAdWordsOfflineConversionSettings
from hip_data_tools.google.adwords import GoogleAdWordsConnectionSettings, \
    GoogleAdWordsSecretsManager


class TestAthenaToAdWordsOfflineConversion(TestCase):
    def test_the_transformation_works(self):
        aws_conn = AwsConnectionSettings(
            region="us-east-1",
            secrets_manager=None,
            profile="default")
        execfile("../../secrets.py")

        cassandra_conn_setting = Mock()

        settings = AthenaToAdWordsOfflineConversionSettings(
            source_database=os.getenv("dummy_athena_database"),
            source_table=os.getenv("dummy_athena_table"),
            source_connection_settings=aws_conn,
            etl_identifier="xxxx",
            destination_batch_size=100,
            etl_state_manager_connection=cassandra_conn_setting,
            etl_state_manager_keyspace="test",
            transformation_column_mapping={'abc': 'googleClickId',
                                           'def1': 'conversionName',
                                           'def2': 'conversionTime',
                                           'def4': 'conversionValue'},
            destination_connection_settings=GoogleAdWordsConnectionSettings(
                client_id=os.getenv("adwords_client_id"),
                user_agent="Tester",
                client_customer_id=os.getenv("adwords_client_customer_id"),
                secrets_manager=GoogleAdWordsSecretsManager()
            ),
        )
        etl = AthenaToAdWordsOfflineConversion(settings)

        df = DataFrame([
            {
                "abc": "123",
                "def1": "123",
                "def2": "123",
                "def3": "123",
                "def4": "123",
                "def5": "123",
            },
            {
                "abc": "222",
                "def1": "333",
                "def2": "444",
                "def3": "333",
                "def4": "333",
                "def5": "333",
            }
        ])
        result = etl._data_frame_to_destination_dict(df)
        expected = [
            {'conversionName': '123',
             'conversionTime': '123',
             'conversionValue': '123',
             'googleClickId': '123'},
            {'conversionName': '333',
             'conversionTime': '444',
             'conversionValue': '333',
             'googleClickId': '222'}
        ]
        self.assertListEqual(result, expected)

    def test_adwords_upload_with_duplicates_in_same_batch(self):
        aws_conn = AwsConnectionSettings(
            region="us-east-1",
            secrets_manager=None,
            profile="default")
        execfile("../../secrets.py")

        compose = DockerCompose(filepath=os.path.dirname(base.__file__))
        with compose:
            host = compose.get_service_host("cassandra", 9042)
            port = int(compose.get_service_port("cassandra", 9042))

            cassandra_conn_setting = CassandraConnectionSettings(
                cluster_ips=[host],
                port=port,
                load_balancing_policy=DCAwareRoundRobinPolicy(),
                secrets_manager=CassandraSecretsManager(source=DictKeyValueSource({
                    "CASSANDRA_USERNAME": "",
                    "CASSANDRA_PASSWORD": "",
                })),
            )

            verify_container_is_up(cassandra_conn_setting)

            settings = AthenaToAdWordsOfflineConversionSettings(
                source_database=os.getenv("dummy_athena_database"),
                source_table=os.getenv("dummy_athena_table"),
                source_connection_settings=aws_conn,
                etl_identifier="xxxx",
                destination_batch_size=100,
                etl_state_manager_connection=cassandra_conn_setting,
                etl_state_manager_keyspace="test",
                transformation_column_mapping={'googleClickId': 'googleClickId',
                                               'conversionName': 'conversionName',
                                               'conversionTime': 'conversionTime',
                                               'conversionValue': 'conversionValue',
                                               'conversionCurrencyCode': 'conversionCurrencyCode'},
                destination_connection_settings=GoogleAdWordsConnectionSettings(
                    client_id=os.getenv("adwords_client_id"),
                    user_agent="Tester",
                    client_customer_id=os.getenv("adwords_client_customer_id"),
                    secrets_manager=GoogleAdWordsSecretsManager()
                ),
            )
            etl = AthenaToAdWordsOfflineConversion(settings)
            test_df = DataFrame(
                [
                    {
                        'googleClickId': 'xxx',
                        'conversionName': 'claim_attempts_testing',
                        'conversionTime': '20200309 074357 UTC',
                        'conversionValue': 17.0,
                        'conversionCurrencyCode': 'AUD',
                    },
                    {
                        'googleClickId': "Cj0KCQiAqY3zBRDQARIsAJeCVxOIyZ8avQ0he3WIpHPwV6hRn"
                                         "-8Y2gDrUBJcc95tDdLcE35TK1mhhmIaAgZGEALw_wcB",
                        'conversionName': 'claim_attempts_testing',
                        'conversionTime': '20200309 074353 UTC',
                        'conversionValue': 17.0,
                        'conversionCurrencyCode': 'AUD',
                    },
                    {
                        'googleClickId': "Cj0KCQiAqY3zBRDQARIsAJeCVxOIyZ8avQ0he3WIpHPwV6hRn"
                                         "-8Y2gDrUBJcc95tDdLcE35TK1mhhmIaAgZGEALw_wcB",
                        'conversionName': 'claim_attempts_testing',
                        'conversionTime': '20200309 074353 UTC',  # Duplicate with same time
                        'conversionValue': 14.0,
                        'conversionCurrencyCode': 'AUD',
                    },
                    {
                        'googleClickId': "Cj0KCQiAqY3zBRDQARIsAJeCVxOIyZ8avQ0he3WIpHPwV6hRn"
                                         "-8Y2gDrUBJcc95tDdLcE35TK1mhhmIaAgZGEALw_wcB",
                        'conversionName': 'claim_attempts_testing',
                        'conversionTime': '20200309 084353 UTC',  # Duplicate with diff time
                        'conversionValue': 14.0,
                        'conversionCurrencyCode': 'AUD',
                    },
                    {
                        'googleClickId': "EAIaIQobChMI6oiGy_vz5wIVkjUrCh3IcgAuEAAYASAAEgLRk_D_BwE",
                        'conversionName': "claim_attempts_testing",
                        'conversionTime': '20200309 023001 UTC',
                        'conversionValue': 17.0,
                        'conversionCurrencyCode': 'AUD',
                    },
                ]
            )

            actual = etl._process_data_frame(test_df)
            expected = [  # The duplicate with same time has been Picked out as an issue
                {
                    'error': "Current State 'EtlStates.Processing' cannot transition to "
                             "'EtlStates.Processing'",
                    'data': {
                        'googleClickId':
                            'Cj0KCQiAqY3zBRDQARIsAJeCVxOIyZ8avQ0he3WIpHPwV6hRn'
                            '-8Y2gDrUBJcc95tDdLcE35TK1mhhmIaAgZGEALw_wcB',
                        'conversionName': 'claim_attempts_testing',
                        'conversionTime': '20200309 074353 UTC', 'conversionValue': 14.0,
                        'conversionCurrencyCode': 'AUD'
                    }
                },
            ]

            self.assertListEqual(actual, expected)

    def test_multiple_runs_of_same_data_and_verify_deduplication(self):
        aws_conn = AwsConnectionSettings(
            region="us-east-1",
            secrets_manager=None,
            profile="default")
        execfile("../../secrets.py")

        compose = DockerCompose(filepath=os.path.dirname(base.__file__))
        with compose:
            host = compose.get_service_host("cassandra", 9042)
            port = int(compose.get_service_port("cassandra", 9042))

            cassandra_conn_setting = CassandraConnectionSettings(
                cluster_ips=[host],
                port=port,
                load_balancing_policy=DCAwareRoundRobinPolicy(),
                secrets_manager=CassandraSecretsManager(source=DictKeyValueSource({
                    "CASSANDRA_USERNAME": "",
                    "CASSANDRA_PASSWORD": "",
                })),
            )

            conn = verify_container_is_up(cassandra_conn_setting)
            # conn.get_session('system').execute(""" DROP TABLE test.etl_sink_record_state""")

            settings = AthenaToAdWordsOfflineConversionSettings(
                source_database=os.getenv("dummy_athena_database"),
                source_table=os.getenv("dummy_athena_table"),
                source_connection_settings=aws_conn,
                etl_identifier="test",
                destination_batch_size=100,
                etl_state_manager_connection=cassandra_conn_setting,
                etl_state_manager_keyspace="test",
                transformation_column_mapping={'google_click_id': 'googleClickId',
                                               'conversion_name': 'conversionName',
                                               'conversion_time': 'conversionTime',
                                               'conversion_value': 'conversionValue',
                                               'conversion_currency_code':
                                                   'conversionCurrencyCode'},
                destination_connection_settings=GoogleAdWordsConnectionSettings(
                    client_id=os.getenv("adwords_client_id"),
                    user_agent="Tester",
                    client_customer_id=os.getenv("adwords_client_customer_id"),
                    secrets_manager=GoogleAdWordsSecretsManager()
                ),
            )
            etl = AthenaToAdWordsOfflineConversion(settings)
            source_data = [
                {
                    'google_click_id': 'theFirst',
                    'conversion_name': 'claim_attempts_testing',
                    'conversion_time': '20200309 074357 UTC',
                    'conversion_value': 17.0,
                    'conversion_currency_code': 'AUD',
                },
                {
                    'google_click_id': 'failedSecond',
                    'conversion_name': 'claim_attempts_testing',
                    'conversion_time': '20200309 074357 UTC',
                    'conversion_value': 17.0,
                    'conversion_currency_code': 'AUD',
                },
            ]
            test_df = DataFrame(source_data)
            #  Mock upload_conversions in AdWordsUtil so no actual data is transmitted
            etl._upload_conversions = MagicMock(return_value=(
                [
                    {
                        'googleClickId': 'theFirst',
                        'conversionName': 'claim_attempts_testing',
                        'conversionTime': '20200309 074357 UTC',
                        'conversionValue': 17.0,
                        'conversionCurrencyCode': 'AUD',
                    },
                ],
                [
                    {
                        'fieldPath': 'operations[0].operand',
                        'fieldPathElements': [
                            {
                                'field': 'operations',
                                'index': 0
                            },
                            {
                                'field': 'operand',
                                'index': None
                            }
                        ],
                        'trigger': None,
                        'errorString': 'OfflineConversionError.UNPARSEABLE_GCLID',
                        'ApiError.Type': 'OfflineConversionError',
                        'reason': 'UNPARSEABLE_GCLID',
                        'data': {
                            'googleClickId': 'failedSecond',
                            'conversionName': 'claim_attempts_testing',
                            'conversionTime': '20200309 074357 UTC',
                            'conversionValue': 17.0,
                            'conversionCurrencyCode': 'AUD',
                        },
                    },
                ]
            )
            )
            # etl._process_data_frame(test_df)
            first_actual = etl._process_data_frame(test_df)
            self.assertListEqual(first_actual, [])

            # Repeat process to cause Duplicates
            actual = etl._process_data_frame(test_df)
            # actual = etl.upload_next()
            expected = [{'data': {'conversionCurrencyCode': 'AUD',
                                  'conversionName': 'claim_attempts_testing',
                                  'conversionTime': '20200309 074357 UTC',
                                  'conversionValue': 17.0,
                                  'googleClickId': 'theFirst'},
                         'error': 'Current state is not Ready'},
                        {'data': {'conversionCurrencyCode': 'AUD',
                                  'conversionName': 'claim_attempts_testing',
                                  'conversionTime': '20200309 074357 UTC',
                                  'conversionValue': 17.0,
                                  'googleClickId': 'failedSecond'},
                         'error': 'Current state is not Ready'}]

            self.assertListEqual(actual, expected)

    def test_full_integration_with_local_cassandra(self):
        aws_conn = AwsConnectionSettings(
            region="us-east-1",
            secrets_manager=None,
            profile="default")
        execfile("../../secrets.py")

        compose = DockerCompose(filepath=os.path.dirname(base.__file__))
        with compose:
            host = compose.get_service_host("cassandra", 9042)
            port = int(compose.get_service_port("cassandra", 9042))

            cassandra_conn_setting = CassandraConnectionSettings(
                cluster_ips=[host],
                port=port,
                load_balancing_policy=DCAwareRoundRobinPolicy(),
                secrets_manager=CassandraSecretsManager(
                    source=DictKeyValueSource({
                        "CASSANDRA_USERNAME": "",
                        "CASSANDRA_PASSWORD": "",
                    })
                ),
            )

            conn = verify_container_is_up(cassandra_conn_setting)
            # conn.get_session('system').execute(""" DROP TABLE test.etl_sink_record_state""")

            settings = AthenaToAdWordsOfflineConversionSettings(
                source_database=os.getenv("dummy_athena_database"),
                source_table=os.getenv("dummy_athena_table"),
                source_connection_settings=aws_conn,
                etl_identifier="test",
                destination_batch_size=100,
                etl_state_manager_connection=cassandra_conn_setting,
                etl_state_manager_keyspace="test",
                transformation_column_mapping={
                    'google_click_id': 'googleClickId',
                    'conversion_name': 'conversionName',
                    'conversion_time': 'conversionTime',
                    'conversion_value': 'conversionValue',
                    'conversion_currency_code': 'conversionCurrencyCode'
                },
                destination_connection_settings=GoogleAdWordsConnectionSettings(
                    client_id=os.getenv("adwords_client_id"),
                    user_agent="Tester",
                    client_customer_id=os.getenv("adwords_client_customer_id"),
                    secrets_manager=GoogleAdWordsSecretsManager()
                ),
            )
            etl = AthenaToAdWordsOfflineConversion(settings)
            files_actual = etl.list_source_files()
            #
            # self.assertListEqual(files_actual, [])

            # etl.upload_all()
            act = etl.upload_all()
            self.assertListEqual(act, [])


@retry(wait_random_max=10, )
def verify_container_is_up(settings):
    conn = CassandraConnectionManager(settings)
    conn.get_session('system').execute("""
        CREATE KEYSPACE IF NOT EXISTS test 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : '1'};""")

    return conn
