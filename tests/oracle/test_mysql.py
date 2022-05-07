from datetime import datetime
from unittest import TestCase
from hip_data_tools.oracle.mysql import prepare_upsert_query


class TestMySqlUtil(TestCase):
    @classmethod
    def setUpClass(cls):
        return

    @classmethod
    def tearDownClass(cls):
        return

    def test__upsert_query_should_work(self):
        test_table = "TEST"
        actual_sql, actual_values = prepare_upsert_query(
            table=test_table,
            primary_keys={
                "pk1": 123,
                "pk2": "abc",
            },
            data={
                "col1": "value1",
                "col2": 2345,
                "col4": datetime(year=2020, month=1, day=1)
            }
        )
        expected_values = (
            123, "abc", "value1", 2345, datetime(year=2020, month=1, day=1))
        expected_sql = f"""
    INSERT INTO {test_table} (pk1, pk2, col1, col2, col4)
    VALUES (%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE col1=%s, col2=%s, col4=%s
    """
        self.assertEqual(actual_sql, expected_sql)
        self.assertEqual(actual_values, expected_values)
