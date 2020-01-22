import datetime
import uuid
from unittest import TestCase
from unittest.mock import Mock

import pandas as pd
from pandas import DataFrame
from pandas._libs.tslibs.nattype import NaT
from pandas.util.testing import assert_frame_equal

from hip_data_tools.apache.cassandra import CassandraUtil


class TestCassandraUtil(TestCase):

    def test__mocked_read_dict__should_call_execute_once(self):
        expected = [{"abc": "def"}, {"abc": "pqr"}]
        mock_result_set = Mock()
        mock_result_set.current_rows = expected
        mock_cassandra_util = Mock()
        mock_cassandra_util.execute = Mock(return_value=mock_result_set)
        actual = CassandraUtil.read_dict(mock_cassandra_util, "SELECT abc FROM def")
        self.assertListEqual(actual, expected)
        mock_cassandra_util.execute.assert_called_once()

    def test__mocked_read_dataframe__should_call_execute_once(self):
        expected = pd.DataFrame([{"abc": "def"}, {"abc": "pqr"}])
        mock_result_set = Mock()
        mock_result_set._current_rows = expected
        mock_cassandra_util = Mock()
        mock_cassandra_util.execute = Mock(return_value=mock_result_set)
        actual = CassandraUtil.read_dataframe(mock_cassandra_util, "SELECT abc FROM def")
        assert_frame_equal(actual, expected)
        mock_cassandra_util.execute.assert_called_once()

    def test___cql_upsert_from_dict__should_generate_proper_cql_string(self):
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
        actual = CassandraUtil._clean_outgoing_values(NaT)
        self.assertEqual(actual, None)

        expected = "some val"
        actual = CassandraUtil._clean_outgoing_values(expected)
        self.assertEqual(actual, expected)

        expected = 1
        actual = CassandraUtil._clean_outgoing_values(expected)
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
        actual = CassandraUtil._extract_rows_from_list_of_dict(data)
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
        actual = CassandraUtil._extract_rows_from_dataframe(df)
        expected = [
            (100, 1234, datetime.datetime(2020, 1, 22, 1, 2), 'this is from a dict'),
            (200, 5678, datetime.datetime(2020, 1, 4, 0), 'this is from a dict'),
            (300, 9, None, 'this is from a dict'),
        ]
        self.assertEqual(actual, expected)
