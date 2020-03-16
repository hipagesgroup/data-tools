import os
from unittest import TestCase, mock

from cassandra.cqlengine import ValidationError
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
    def integration_test__the_etl_state_management_should_go_through_all_state_sucessfully_and_persist(
        self):
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

    def test__state_manager_transitions_should_fail_ready_to_succeded(self):
        with mock.patch.object(EtlSinkRecordStateManager, '_get_or_create_state',
                               return_value={"record_state": EtlStates.Ready}):
            m = EtlSinkRecordStateManager("abc", "def")
            with self.assertRaises(ValidationError):
                m.succeeded()

    def test__state_manager_transitions_should_fail_ready_to_falied(self):
        with mock.patch.object(EtlSinkRecordStateManager, '_get_or_create_state',
                               return_value={"record_state": EtlStates.Ready}):
            m = EtlSinkRecordStateManager("abc", "def")
            with self.assertRaises(ValidationError):
                m.failed()

    def test__state_manager_transitions_should_be_ok_ready_to_processing(self):
        with mock.patch.object(EtlSinkRecordStateManager, '_get_or_create_state',
                               return_value={"record_state": EtlStates.Ready}):
            m = EtlSinkRecordStateManager("abc", "def")
            with mock.patch(
                "hip_data_tools.etl.common.EtlSinkRecordState") as MockedEtlSinkRecordState:
                m.processing()
                MockedEtlSinkRecordState.update.assert_called_once()

    def test__state_manager_transitions_should_be_ok_processing_to_succeded(self):
        with mock.patch.object(EtlSinkRecordStateManager, '_get_or_create_state',
                               return_value={"record_state": EtlStates.Processing}):
            m = EtlSinkRecordStateManager("abc", "def")
            with mock.patch(
                "hip_data_tools.etl.common.EtlSinkRecordState") as MockedEtlSinkRecordState:
                m.succeeded()
                MockedEtlSinkRecordState.update.assert_called_once()

    def test__state_manager_transitions_should_be_ok_processing_to_failed(self):
        with mock.patch.object(EtlSinkRecordStateManager, '_get_or_create_state',
                               return_value={"record_state": EtlStates.Processing}):
            m = EtlSinkRecordStateManager("abc", "def")
            with mock.patch(
                "hip_data_tools.etl.common.EtlSinkRecordState") as MockedEtlSinkRecordState:
                m.succeeded()
                MockedEtlSinkRecordState.update.assert_called_once()

    def test__state_manager_transitions_should_fail_after_succeded(self):
        with mock.patch.object(EtlSinkRecordStateManager, '_get_or_create_state',
                               return_value={"record_state": EtlStates.Succeeded}):
            m = EtlSinkRecordStateManager("abc", "def")
            with self.assertRaises(ValidationError):
                m.ready()
            with self.assertRaises(ValidationError):
                m.processing()
            with self.assertRaises(ValidationError):
                m.failed()
            with self.assertRaises(ValidationError):
                m.succeeded()

    def test__state_manager_transitions_should_fail_to_ready(self):
        with mock.patch.object(EtlSinkRecordStateManager, '_get_or_create_state',
                               return_value={"record_state": EtlStates.Failed}):
            m = EtlSinkRecordStateManager("abc", "def")
            with self.assertRaises(ValidationError):
                m.processing()
            with self.assertRaises(ValidationError):
                m.succeeded()
            with self.assertRaises(ValidationError):
                m.failed()
            with mock.patch(
                "hip_data_tools.etl.common.EtlSinkRecordState") as MockedEtlSinkRecordState:
                m.ready()
                MockedEtlSinkRecordState.update.assert_called_once()


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
