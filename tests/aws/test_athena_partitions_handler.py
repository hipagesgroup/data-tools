from unittest import TestCase

from hip_data_tools.aws.athena import AthenaTablePartitionsHandlerUtil, \
    AthenaTablePartitionsHandlerSettings


class TestAthenaTablePartitionsHandlerUtil(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.athena_partition_util = AthenaTablePartitionsHandlerUtil(
            settings=AthenaTablePartitionsHandlerSettings(
                database='test',
                conn=None,
                output_bucket='test_output_bucket',
                output_key='test/',
                table='test_table',
                s3_bucket='test_bucket',
                s3_key='test/data/partition_1=value_1/partition_2=value_2/',
                partition_col_names=['partition_1', 'partition_2', 'partition_3', 'partition_4'],
                key_suffix=None))

    def test__get_dir_path_list__should__return_directory_paths__when__key_list_is_given(self):
        actual = self.athena_partition_util._get_dir_path_list(key_list=[
            'data/external/source=test_source/test_source_app_name=consumer/report=test_report/date_dim_key=20200505/file-b5354d0a-f239-4908-9bbc-1c8fd3e88f94.csv.gz',
            'data/external/source=test_source/test_source_app_name=consumer/report=test_report/date_dim_key=20200506/file-ae4101e4-f082-4e9e-ba31-bc77297cb14a.csv.gz',
            'data/external/source=test_source/test_source_app_name=consumer/report=test_report/date_dim_key=20200508/file-0d95e5f0-1512-46ee-9129-935c358ee698.csv.gz',
            'data/external/source=test_source/test_source_app_name=consumer/report=test_report/date_dim_key=20200509/file-b62a1855-0890-44be-b2d0-27058538e7f6.csv.gz',
            'data/external/source=test_source/test_source_app_name=consumer/report=test_report/date_dim_key=20200510/file-e41e8bdf-23f2-4cb9-9dad-bc93fd67a0ad.csv.gz',
            'data/external/source=test_source/test_source_app_name=consumer/report=test_report/date_dim_key=20200514/file-6f403f7c-0b3e-43f7-a14b-8d5734e08e96.csv.gz'])
        expected = [
            'data/external/source=test_source/test_source_app_name=consumer/report=test_report/date_dim_key=20200505',
            'data/external/source=test_source/test_source_app_name=consumer/report=test_report/date_dim_key=20200506',
            'data/external/source=test_source/test_source_app_name=consumer/report=test_report/date_dim_key=20200508',
            'data/external/source=test_source/test_source_app_name=consumer/report=test_report/date_dim_key=20200509',
            'data/external/source=test_source/test_source_app_name=consumer/report=test_report/date_dim_key=20200510',
            'data/external/source=test_source/test_source_app_name=consumer/report=test_report/date_dim_key=20200514']
        self.assertEqual(actual, expected)

    def test__get_add_partitions_query_for_chunk__should__return_query__when__list_of_dicts_are_given(
            self):
        actual = self.athena_partition_util._get_add_partitions_query_for_chunk(chunk=[
            {'partition_1': 'test_source', 'partition_2': 'consumer',
             'partition_3': 'test_report_1',
             'partition_4': '20190822'},
            {'partition_1': 'test_source', 'partition_2': 'consumer',
             'partition_3': 'test_report_2',
             'partition_4': '20190311'},
            {'partition_1': 'test_source', 'partition_2': 'consumer',
             'partition_3': 'test_report_3',
             'partition_4': '20190702'},
            {'partition_1': 'test_source', 'partition_2': 'consumer',
             'partition_3': 'test_report_4',
             'partition_4': '20200312'},
            {'partition_1': 'test_source', 'partition_2': 'consumer',
             'partition_3': 'test_report_5',
             'partition_4': '20200229'},
            {'partition_1': 'test_source', 'partition_2': 'consumer',
             'partition_3': 'test_report_6',
             'partition_4': '20200230'}])
        expected = """ALTER TABLE test_table ADD IF NOT EXISTS PARTITION (partition_1 = 'test_source', partition_2 = 'consumer', partition_3 = 'test_report_1', partition_4 = '20190822') PARTITION (partition_1 = 'test_source', partition_2 = 'consumer', partition_3 = 'test_report_2', partition_4 = '20190311') PARTITION (partition_1 = 'test_source', partition_2 = 'consumer', partition_3 = 'test_report_3', partition_4 = '20190702') PARTITION (partition_1 = 'test_source', partition_2 = 'consumer', partition_3 = 'test_report_4', partition_4 = '20200312') PARTITION (partition_1 = 'test_source', partition_2 = 'consumer', partition_3 = 'test_report_5', partition_4 = '20200229') PARTITION (partition_1 = 'test_source', partition_2 = 'consumer', partition_3 = 'test_report_6', partition_4 = '20200230');"""
        self.assertEqual(actual, expected)

    def test__get_partition_dict__should__give_dict_of_partitions__when__a_s3_key_is_given(self):
        actual = self.athena_partition_util._get_partition_dict(
            s3_prefix='data/external/source=test_source/test_source_app_name=consumer/report=test_report/date_dim_key=20200505')
        expected = {'date_dim_key': '20200505',
                    'report': 'test_report',
                    'source': 'test_source',
                    'test_source_app_name': 'consumer'}
        self.assertEqual(actual, expected)
