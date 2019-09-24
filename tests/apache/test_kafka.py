import os

import mock
import pandas as pd
from freezegun import freeze_time
from pandas.testing import assert_frame_equal

import hip_data_tools.apache.kafka as kafka
from hip_data_tools.apache.kafka import get_from_env_or_default_with_warning

broker_address_and_port = 'foo:9092'


class StubbedKafkaMessageObject:
    """
    Stubbed class which allows testing of of the Kafka Message objects

    """

    def __init__(self, payload, timestamp):
        self.payload = payload
        self.timestamp_to_return = timestamp

    def timestamp(self):
        """
        Stubbed method to return a fixed timestamp
        Returns: timestamp
        """
        return self.timestamp_to_return

    def value(self):
        """The messages are string byte streams encoded with uft-8, so here
        we byte encode the payload to mimic the kafka messages"""
        return self.payload.encode('utf-8')


@mock.patch.dict(os.environ, {'FOO': broker_address_and_port})
def test_env_getter_should_ReadFromEnv():
    default_value = 'localhost:9092'
    value = get_from_env_or_default_with_warning('FOO',
                                                 default_value)
    assert value == broker_address_and_port


@freeze_time("2012-01-14 03:21:34")
def test_fld_nm_getter_Should_GenerateTimeStampedFileNameUsingOSClock():
    expected_folder_name = 'df_dt_of_upload_20120114_000000.parquet'

    fld_name = kafka.generate_snapshot_file_name_with_timestamp()

    assert fld_name == expected_folder_name


def test_JsonConverter_Should_ParseGoodData_When_PassedCorrectJson():
    list_of_json_payloads = ['{"field_1": '
                             '"fab4c9dd-bf3c-45b0-b88b-89e978eec436",'
                             ' "field_2": '
                             '"ba0421ff-b62c-4dcb-98b9-fc2f969b4325"}',
                             '{"field_1": '
                             '"4ab1687d-ed8d-424f-b226-b6af98055df1",'
                             ' "field_2": '
                             '"9a9de7c0-d57c-4fb6-9fd5-ca7f406c2c7f"}']

    list_of_timestamps = ["2012-01-14 03:21:34", "2014-02-11 05:21:24"]

    kafka_msgs = []

    for payload, timestamp in zip(list_of_json_payloads, list_of_timestamps):
        kafka_msgs.append(StubbedKafkaMessageObject(payload, timestamp))

    good_data, bad_data = kafka.convert_msgs_to_dictionary(kafka_msgs)

    assert (len(good_data) == 2)


def test_JsonConverter_Should_ReturnBadData_When_PassedNonJsonData():
    list_of_json = ['{"field_1": "fab4c9dd-bf3c-45b0-b88b-89e978eec436",'
                    ' "field_2": "ba0421ff-b62c-4dcb-98b9-fc2f969b4325"}',
                    '{"field_1": "4ab1687d-ed8d-424f-b226-b6af98055df1",'
                    ' "field_2": "9a9de7c0-d57c-4fb6-9fd5-ca7f406c2c7f"}',
                    'oh no, some bad data that, not correctly json formatted!']

    list_of_timestamps = ["2012-01-14 03:21:34",
                          "2014-02-11 05:21:24",
                          "2015-02-11 15:26:24"]

    kafka_msgs = []

    for payload, timestamp in zip(list_of_json, list_of_timestamps):
        kafka_msgs.append(StubbedKafkaMessageObject(payload, timestamp))

    good_data, bad_data = kafka.convert_msgs_to_dictionary(kafka_msgs)

    assert (len(good_data) == 2)
    assert (len(bad_data) == 1)


def test_DataFramePartitioner_Should_SplitTheDF_by_timestamps():
    list_of_dicts = [{'field1': 'some string value',
                      'field2': "1490195805000"},
                     {'field1': 'another string value',
                      'field2': "1490198805000"},
                     {'field1': 'string value 3',
                      'field2': "1490198805000"}]

    df_in = pd.DataFrame(list_of_dicts)
    df = kafka.add_interval_partitioning_column(df_in,
                                                'field2',
                                                'partition_key_ts',
                                                5)

    expected_data = [{'field1': 'some string value',
                      'field2': "1490195805000",
                      'partition_key_ts': "2017-03-22 15:16:45"},
                     {'field1': 'another string value',
                      'field2': "1490198805000",
                      'partition_key_ts': "2017-03-22 16:06:45"},
                     {'field1': 'string value 3',
                      'field2': "1490198805000",
                      'partition_key_ts': "2017-03-22 16:06:45"}, ]

    df_expected = pd.DataFrame(expected_data)

    df_expected['field2'] = \
        pd.to_datetime(df_expected['field2'], unit='ms')

    df_expected['partition_key_ts'] = \
        pd.to_datetime(df_expected['partition_key_ts'])

    assert_frame_equal(df, df_expected)


@freeze_time("2012-01-14 03:21:34")
def test_PartitioningOfMessages_Should_SplitMessagesAndUploadToS3(mocker):
    class StubS3:

        def upload_df_to_s3(self, df_partition, full_path_on_s3):
            return None

    list_of_dicts = [{'field1': 'some string value',
                      'field2': "2012-01-14 03:21:33",
                      kafka.DEFAULT_KAFKA_TIMESTAMP_COLUMN_NAME:
                          "1490195805000"},
                     {'field1': 'another string value',
                      'field2': "2012-01-14 03:21:34",
                      kafka.DEFAULT_KAFKA_TIMESTAMP_COLUMN_NAME:
                          "1490195805000"},
                     {'field1': 'string value 3',
                      'field2': "2012-01-14 03:21:45",
                      kafka.DEFAULT_KAFKA_TIMESTAMP_COLUMN_NAME:
                          "1490197805000"}]

    kafka_s3_batch_exporter = \
        kafka.KafkaS3BatchExporter("some_path",
                                   StubS3(),
                                   kafka.DEFAULT_KAFKA_TIMESTAMP_COLUMN_NAME,
                                   kafka.DEFAULT_TIMESTAMP_PARTITION_KEY)

    paths_and_df = \
        kafka_s3_batch_exporter.partition_msgs_and_locations(list_of_dicts, 10)

    assert (len(paths_and_df) == 2)

    expected_path = \
        'some_path/date_of_batch=20170322/time_of_batch=151650/' + \
        'df_dt_of_upload_20120114_000000.parquet'

    assert (paths_and_df[0][1] == expected_path)

    assert (paths_and_df[0][0].shape == (2, 4))
