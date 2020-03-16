import os
from unittest import TestCase

from cassandra.policies import DCAwareRoundRobinPolicy
from retrying import retry
from testcontainers.compose import DockerCompose

import tests.test_common as base
from hip_data_tools.apache.cassandra import CassandraConnectionManager, \
    CassandraConnectionSettings, \
    CassandraSecretsManager
from hip_data_tools.common import DictKeyValueSource
from hip_data_tools.etl.common import EtlSinkRecordStateManager, sync_etl_state_table, EtlStates


class TestCommonEtl(TestCase):
    def test__the_etl_state_management_should_go_through_all_state_sucessfully_and_persist(self):
        compose = DockerCompose(filepath=os.path.dirname(base.__file__))
        with compose:
            host = compose.get_service_host("cassandra", 9042)
            port = int(compose.get_service_port("cassandra", 9042))

            conn = connect(host, port)

            conn.setup_connection("dev")

            sync_etl_state_table()
            sm = EtlSinkRecordStateManager(
                record_identifier="RandomRecordId",
                etl_signature="SomeTestEtl"
            )
            print(sm.current_state())
            print(sm.remote_state)
            self.assertEqual(sm.current_state(), EtlStates.Ready)
            sm.processing()
            self.assertEqual(sm.current_state(), EtlStates.Processing)
            sm.failed()
            self.assertEqual(sm.current_state(), EtlStates.Failed)
            sm.ready()
            self.assertEqual(sm.current_state(), EtlStates.Ready)
            sm.processing()
            self.assertEqual(sm.current_state(), EtlStates.Processing)
            sm.succeeded()
            self.assertEqual(sm.current_state(), EtlStates.Succeeded)


@retry(wait_random_max=10, )
def connect(host, port):
    conn = CassandraConnectionManager(
        CassandraConnectionSettings(
            cluster_ips=[host],
            port=port,
            load_balancing_policy=DCAwareRoundRobinPolicy(),
            secrets_manager=CassandraSecretsManager(source=DictKeyValueSource({
                "CASSANDRA_USERNAME": "",
                "CASSANDRA_PASSWORD": "",
            })),
        )
    )
    conn.get_session('system').execute("""
        CREATE KEYSPACE IF NOT EXISTS dev 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : '1'};""")
    return conn
