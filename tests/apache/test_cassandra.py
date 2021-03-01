import datetime
import os
import uuid
from unittest import TestCase
from unittest.mock import Mock

import pandas as pd
from pandas import DataFrame
from pandas._libs.tslibs.nattype import NaT
from pandas.util.testing import assert_frame_equal

from hip_data_tools.apache.cassandra import CassandraUtil, dataframe_to_cassandra_tuples, \
    _standardize_datatype, dicts_to_cassandra_tuples, CassandraSecretsManager, \
    _get_data_frame_column_types, get_cql_columns_from_dataframe


class TestCassandraUtil(TestCase):

    def test__mocked_read_dict__should_call_execute_once(self):
        expected = [{"abc": "def"}, {"abc": "pqr"}]
        mock_result_set = Mock()
        mock_result_set.current_rows = expected
        mock_cassandra_util = Mock()
        mock_cassandra_util.execute = Mock(return_value=mock_result_set)
        actual = CassandraUtil.read_as_dictonary_list(mock_cassandra_util, "SELECT abc FROM def")
        self.assertListEqual(actual, expected)
        mock_cassandra_util.execute.assert_called_once()

    def test__mocked_read_dataframe__should_call_execute_once(self):
        expected = pd.DataFrame([{"abc": "def"}, {"abc": "pqr"}])
        mock_result_set = Mock()
        mock_result_set._current_rows = expected
        mock_cassandra_util = Mock()
        mock_cassandra_util.execute = Mock(return_value=mock_result_set)
        actual = CassandraUtil.read_as_dataframe(mock_cassandra_util, "SELECT abc FROM def")
        assert_frame_equal(actual, expected)
        mock_cassandra_util.execute.assert_called_once()

    def test___cql_upsert_from_dict__should_generate_proper_cql_string(self):
        upsert_dict = {
            "example_type": 100,
            "example_id": uuid.uuid4(),
            "created_at": datetime.datetime.now(),
            "description": "this is from a dict"
        }
        mock_cassandra_util = Mock()
        mock_cassandra_util.keyspace = "test"
        actual = CassandraUtil._cql_upsert_from_dict(mock_cassandra_util, upsert_dict, "test")
        expected = """
        INSERT INTO test.test 
        (example_type, example_id, created_at, description) 
        VALUES (?, ?, ?, ?);
        """
        self.assertEqual(actual.strip(), expected.strip())

    def test___cql_upsert_from_dataframe__should_generate_proper_cql_string(self):
        upsert_dict = [
            {
                "example_type": 100,
                "example_id": uuid.uuid4(),
                "created_at": datetime.datetime.now(),
                "description": "this is from a dict"
            },
            {
                "example_type": 200,
                "example_id": uuid.uuid4(),
                "created_at": datetime.datetime.now(),
                "description": "this is from a dict"
            },
            {
                "example_type": 300,
                "example_id": uuid.uuid4(),
                "created_at": None,
                "description": "this is from a dict"
            }, ]
        upsert_df = DataFrame(upsert_dict)
        mock_cassandra_util = Mock()
        mock_cassandra_util.keyspace = "test"
        actual = CassandraUtil._cql_upsert_from_dataframe(mock_cassandra_util, upsert_df, "test")
        expected = """
        INSERT INTO test.test 
        (example_type, example_id, created_at, description) 
        VALUES (?, ?, ?, ?);
        """
        self.assertEqual(actual.strip(), expected.strip())

    def test___clean_outgoing_values__should_do_its_cleaning(self):
        actual = _standardize_datatype(NaT)
        self.assertEqual(actual, None)

        expected = "some val"
        actual = _standardize_datatype(expected)
        self.assertEqual(actual, expected)

        expected = 1
        actual = _standardize_datatype(expected)
        self.assertEqual(actual, expected)

    def test___extract_rows_from_list_of_dict__should_form_correct_matrix(self):
        data = [
            {
                "example_type": 100,
                "example_id": 1234,
                "created_at": datetime.datetime(2020, 1, 22),
                "description": "this is from a dict"
            },
            {
                "example_type": 200,
                "example_id": 5678,
                "created_at": datetime.datetime(2020, 1, 22),
                "description": "this is from a dict"
            },
            {
                "example_type": 300,
                "example_id": 9,
                "created_at": None,
                "description": "this is from a dict"
            }, ]
        actual = dicts_to_cassandra_tuples(data)
        expected = [
            (100, 1234, datetime.datetime(2020, 1, 22), 'this is from a dict'),
            (200, 5678, datetime.datetime(2020, 1, 22), 'this is from a dict'),
            (300, 9, None, 'this is from a dict'),
        ]
        self.assertEqual(actual, expected)

    def test___extract_rows_from_dataframe__should_form_correct_matrix(self):
        data = [
            {
                "example_type": 100,
                "example_id": 1234,
                "created_at": datetime.datetime(2020, 1, 22, 1, 2),
                "description": "this is from a dict"
            },
            {
                "example_type": 200,
                "example_id": 5678,
                "created_at": datetime.datetime(2020, 1, 4),
                "description": "this is from a dict"
            },
            {
                "example_type": 300,
                "example_id": 9,
                "created_at": None,
                "description": "this is from a dict"
            }, ]
        df = DataFrame(data)
        actual = dataframe_to_cassandra_tuples(df)
        expected = [
            (100, 1234, datetime.datetime(2020, 1, 22, 1, 2), 'this is from a dict'),
            (200, 5678, datetime.datetime(2020, 1, 4, 0), 'this is from a dict'),
            (300, 9, None, 'this is from a dict'),
        ]
        self.assertEqual(actual, expected)

    def test___extract_rows_from_dataframe__should_work_with_float_and_int(self):
        data = [
            {
                "example_type": 100.0,
                "example_id": 1234,
                "created_at": datetime.datetime(2020, 1, 22, 1, 2),
                "description": "this is from a dict"
            },
            {
                "example_type": 200.0,
                "example_id": 5678,
                "created_at": datetime.datetime(2020, 1, 4),
                "description": "this is from a dict"
            }, ]
        df = DataFrame(data)
        actual = dataframe_to_cassandra_tuples(df)
        for actual_tuple in actual:
            self.assertTrue(isinstance(actual_tuple[0], float))
            self.assertTrue(isinstance(actual_tuple[1], int))
            self.assertTrue(isinstance(actual_tuple[2], datetime.datetime))
            self.assertTrue(isinstance(actual_tuple[3], str))

    def test__cassandra_secrets_manager_should_raise_errors_when_keys_are_not_found(self):
        def func():
            CassandraSecretsManager(username_var="SOMEUNKNOWNVAR")

        self.assertRaises(Exception, func)

    def test__cassandra_secrets_manager_should_instantiate_with_sensible_defaults(self):
        os.environ["CASSANDRA_USERNAME"] = "abc"
        os.environ["CASSANDRA_PASSWORD"] = "def"
        actual = CassandraSecretsManager()
        self.assertEqual(actual.username, "abc")
        self.assertEqual(actual.password, "def")

    def test__get_data_frame_column_types__should_work(self):
        data = [
            {
                "example_type": 100,
                "example_id": 1234,
                "created_at": datetime.datetime(2020, 1, 22, 1, 2),
                "description": "this is from a dict",
                "abc": {"something": "foo"},
                "abc2": -1,
                "abc3": 0.23456,
            },
            {
                "example_type": 100,
                "example_id": 1234,
                "created_at": datetime.datetime(2020, 1, 22, 1, 2),
                "description": "this is from a dict",
                "abc": {"something": "foo"},
                "abc2": -1,
                "abc3": 0.23456,
            },
        ]
        df = DataFrame(data)
        result = _get_data_frame_column_types(df)
        self.assertDictEqual(result, {'created_at': 'Timestamp',
                                      'description': 'str',
                                      'example_id': 'int64',
                                      'example_type': 'int64',
                                      'abc': 'dict',
                                      'abc2': 'int64',
                                      'abc3': 'float64',
                                      })

    def test__convert_dataframe_columns_to_cassandra__should_work(self):
        data = [
            {
                "example_type": 100,
                "example_id": 1234,
                "created_at": datetime.datetime(2020, 1, 22, 1, 2),
                "description": "this is from a dict",
                "abc": {"something": "foo"},
                "abc2": -1,
                "abc3": 0.23456,
            },
            {
                "example_type": 100,
                "example_id": 1234,
                "created_at": datetime.datetime(2020, 1, 22, 1, 2),
                "description": "this is from a dict",
                "abc": {"something": "foo"},
                "abc2": -1,
                "abc3": 0.23456,
            },
        ]
        df = DataFrame(data)
        result = get_cql_columns_from_dataframe(df)
        self.assertDictEqual(result, {'abc': 'map',
                                      'abc2': 'bigint',
                                      'abc3': 'double',
                                      'created_at': 'timestamp',
                                      'description': 'varchar',
                                      'example_id': 'bigint',
                                      'example_type': 'bigint',
                                      })

    def test___dataframe_to_cassandra_ddl_should_produce_proper_cql(self):
        data = [
            {
                "abc": {"something": "foo"},
                "abc2": -1,
                "abc3": 0.23456,
            },
            {
                "abc": {"something": "foo"},
                "abc2": -1,
                "abc3": 0.23456,
            },
        ]
        df = DataFrame(data)
        mock_cassandra_util = Mock()
        mock_cassandra_util.keyspace = "test"
        actual = CassandraUtil._dataframe_to_cassandra_ddl(
            mock_cassandra_util, df,
            destination_table_partition_key=["abc"],
            destination_table_clustering_keys=["abc2"],
            table_name="test",
            table_options_statement=""
        )
        expected = """
        CREATE TABLE IF NOT EXISTS test.test (
            abc map, abc2 bigint, abc3 double,
            PRIMARY KEY ((abc),abc2))
        ;
        """
        self.assertEqual(actual, expected)

        actual = CassandraUtil._dataframe_to_cassandra_ddl(
            mock_cassandra_util, df,
            destination_table_partition_key=["abc", "abc2"],
            destination_table_clustering_keys=[],
            table_name="test",
            table_options_statement="WITH comments = 'some text that describes the table'"
        )
        expected = """
        CREATE TABLE IF NOT EXISTS test.test (
            abc map, abc2 bigint, abc3 double,
            PRIMARY KEY ((abc, abc2)))
        WITH comments = 'some text that describes the table';
        """
        self.assertEqual(actual, expected)

    def test__prepare_batches__should_provide_correct_number_of_batches(self):
        input = [
            ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"),
            ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"),
            ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"),
            ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"),
            ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"),
            ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"),
            ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"),
            ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"),
            ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"),
            ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"),
            ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"),
            ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"),
            ("abc", 123, "def"), ("abc", 123, "def"), ("abc", 123, "def"),
        ]

        expected = ["MockBatch", "MockBatch", "MockBatch"]
        mock_cassandra_util = Mock()
        mock_cassandra_util._prepare_batch = Mock(return_value="MockBatch")
        prepared_statement = Mock()
        actual = CassandraUtil.prepare_batches(mock_cassandra_util, prepared_statement, input,
                                               batch_size=20)
        self.assertEqual(actual, expected)
