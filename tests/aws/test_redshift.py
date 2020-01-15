from unittest import TestCase

from hip_data_tools.aws.redshift import RedshiftUtil


class TestRedshiftUtil(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ru = RedshiftUtil(host="wbh1.cqdmrqjbi4nz.us-east-1.redshift.amazonaws.com", port="5439", schema="dwh",
                              user="user", password="password")
        # cls.ru = RedshiftUtil(schema="dwh", redshift_conn_id="postgres_dwh_write")

    def test_get_simplified_datatype_integers(self):
        actual = self.ru.simplified_dtype("TIMESTAMP WITHOUT TIME ZONE")
        expected = "TIMESTAMP"
        self.assertEqual(actual, expected)
        actual = self.ru.simplified_dtype("NUMERIC(9,6)")
        expected = "NUMERIC"
        self.assertEqual(actual, expected)
        actual = self.ru.simplified_dtype("character varying(4096)")
        expected = "CHARACTER"
        self.assertEqual(actual, expected)
        actual = self.ru.simplified_dtype("CHARACTER(6)")
        expected = "CHARACTER"
        self.assertEqual(actual, expected)
        actual = self.ru.simplified_dtype("REAL")
        expected = "REAL"
        self.assertEqual(actual, expected)

    def test_convert_dtype_athena_csv(self):
        actual = self.ru.convert_dtype_athena("integer", file_format='CSV')
        expected = "BIGINT"
        self.assertEqual(actual, expected)
        actual = self.ru.convert_dtype_athena("bigint", file_format='CSV')
        expected = "BIGINT"
        self.assertEqual(actual, expected)
        actual = self.ru.convert_dtype_athena("numeric(9,3)", file_format='CSV')
        expected = "DOUBLE"
        self.assertEqual(actual, expected)
        actual = self.ru.convert_dtype_athena("double precision", file_format='CSV')
        expected = "DOUBLE"
        self.assertEqual(actual, expected)
        actual = self.ru.convert_dtype_athena("character varying(65535)", file_format='CSV')
        expected = "STRING"
        self.assertEqual(actual, expected)

    def test_convert_dtype_athena_parquet(self):
        actual = self.ru.convert_dtype_athena("integer", file_format='PARQUET')
        expected = "INTEGER"
        self.assertEqual(actual, expected)
        actual = self.ru.convert_dtype_athena("bigint", file_format='PARQUET')
        expected = "BIGINT"
        self.assertEqual(actual, expected)
        actual = self.ru.convert_dtype_athena("numeric(9,3)", file_format='PARQUET')
        expected = "DECIMAL(9,3)"
        self.assertEqual(actual, expected)
        actual = self.ru.convert_dtype_athena("double precision", file_format='PARQUET')
        expected = "DOUBLE"
        self.assertEqual(actual, expected)
        actual = self.ru.convert_dtype_athena("character varying(65535)", file_format='PARQUET')
        expected = "STRING"
        self.assertEqual(actual, expected)
        actual = self.ru.convert_dtype_athena("SMALLINT", file_format='PARQUET')
        expected = "SMALLINT"
        self.assertEqual(actual, expected)
        actual = self.ru.convert_dtype_athena("REAL", file_format='PARQUET')
        expected = "BIGINT"
        self.assertEqual(actual, expected)
        actual = self.ru.convert_dtype_athena("boolean", file_format='PARQUET')
        expected = "BOOLEAN"
        self.assertEqual(actual, expected)
        actual = self.ru.convert_dtype_athena("character", file_format='PARQUET')
        expected = "STRING"
        self.assertEqual(actual, expected)
        actual = self.ru.convert_dtype_athena("timestamp", file_format='PARQUET')
        expected = "TIMESTAMP"
        self.assertEqual(actual, expected)
        actual = self.ru.convert_dtype_athena("date", file_format='PARQUET')
        expected = "DATE"
        self.assertEqual(actual, expected)
