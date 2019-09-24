import os
import uuid

import mock
import pandas as pd
import pytest
from freezegun import freeze_time
from moto import mock_s3
from pandas.testing import assert_frame_equal

import hip_data_tools.apache.kafka as kafka

BOOTSTRAP_SERVERS = 'somehost:9092'
PRODUCER_CONFIG = \
    """{'queue.buffering.max.messages': 20000, 'queue.buffering.max.ms' : 
    2000}"""
GROUP_ID = str(uuid.uuid4())

CONSUEMR_CONF = """{'auto.offset.reset': 'latest'}"""
KAFKA_TOPIC = 'some_topic'
KAFKA_TIMEOUT_INTERVAL = 100
KAFKA_POLLING_INTERVAL = 200

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


@mock.patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
                              "KAFKA_PRODUCER_CONF": PRODUCER_CONFIG})
def test__producer_config_should_read_from_env():
    kafka_producer_config = kafka.ProducerConfig()
    configs_dict = {'queue.buffering.max.messages': 20000,
                    'queue.buffering.max.ms': 2000,
                    'bootstrap.servers': 'somehost:9092'}

    assert kafka_producer_config.configs == configs_dict
    assert kafka_producer_config._bootstrap_servers == BOOTSTRAP_SERVERS


@mock.patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
                              "KAFKA_PRODUCER_CONF": PRODUCER_CONFIG})
def test__producer_creator_should_create_from_env_when_nothing_is_passed():
    kafka_producer = kafka.create_producer()

    assert type(kafka_producer).__name__ == "Producer"


@mock.patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
                              "KAFKA_CONSUMER_CONF": CONSUEMR_CONF,
                              "KAFKA_CONSUMER_GROUP_ID": GROUP_ID})
def test__consumer_config_should_read_from_env():
    kafka_producer_config = kafka.ConsumerConfig()

    configs_dict = {'auto.offset.reset': 'latest',
                    'bootstrap.servers': BOOTSTRAP_SERVERS,
                    'group.id': GROUP_ID}

    assert kafka_producer_config.configs == configs_dict
    assert kafka_producer_config._bootstrap_servers == BOOTSTRAP_SERVERS


@mock.patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
                              "KAFKA_CONSUMER_CONF": CONSUEMR_CONF,
                              "KAFKA_CONSUMER_GROUP_ID": GROUP_ID})
def test__consumer_config_should_read_from_env():
    kafka_consumer = kafka.create_consumer()

    assert type(kafka_consumer).__name__ == "Consumer"


@mock.patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
                              "KAFKA_CONSUMER_CONF": CONSUEMR_CONF,
                              "KAFKA_CONSUMER_GROUP_ID": GROUP_ID,
                              "KAFKA_TOPIC": KAFKA_TOPIC,
                              "KAFKA_TIMEOUT_INTERVAL":
                                  str(KAFKA_TIMEOUT_INTERVAL)})
def test__poller_should_create_from_env():
    poller = kafka.create_poller()

    assert type(poller).__name__ == "KafkaPoller"


@mock_s3
@mock.patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
                              "KAFKA_CONSUMER_CONF": CONSUEMR_CONF,
                              "KAFKA_CONSUMER_GROUP_ID": GROUP_ID,
                              "KAFKA_TOPIC": KAFKA_TOPIC,
                              "KAFKA_TIMEOUT_INTERVAL":
                                  str(KAFKA_TIMEOUT_INTERVAL),
                              "KAFKA_POLLING_INTERVAL":
                                  str(KAFKA_POLLING_INTERVAL),
                              "KAFKA_EXPORT_PATH": "some_path",
                              "KAFKA_EXPORT_BUCKET": "a_bucket",
                              "KAFKA_TS_COL_NM": "ts_column",
                              "KAFKA_PARTITION_KEY_NM": 'partition_column'})
def test__conduit_should_create_from_env():
    conduit = kafka.create_conduit()

    assert type(conduit).__name__ == "KafkaConduit"


@mock_s3
@mock.patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
                              "KAFKA_CONSUMER_CONF": CONSUEMR_CONF,
                              "KAFKA_CONSUMER_GROUP_ID": GROUP_ID,
                              "KAFKA_TOPIC": KAFKA_TOPIC,
                              "KAFKA_TIMEOUT_INTERVAL":
                                  str(KAFKA_TIMEOUT_INTERVAL),
                              "KAFKA_POLLING_INTERVAL":
                                  str(KAFKA_POLLING_INTERVAL),
                              "KAFKA_EXPORT_PATH": "some_path",
                              "KAFKA_EXPORT_BUCKET": "a_bucket",
                              "KAFKA_TS_COL_NM": "ts_column",
                              "KAFKA_PARTITION_KEY_NM": 'partition_column'})
def test__conduit_should_create_from_env():
    conduit = kafka.create_batch_s3_uploader()

    assert type(conduit).__name__ == "KafkaS3BatchExporter"


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
        kafka_s3_batch_exporter.partition_msgs_by_kafka_ts(list_of_dicts, 10)

    assert (len(paths_and_df) == 2)

    expected_path = \
        'some_path/date_of_batch=20170322/time_of_batch=151650/' + \
        'df_dt_of_upload_20120114_000000.parquet'

    assert (paths_and_df[0][1] == expected_path)

    assert (paths_and_df[0][0].shape == (2, 4))


@mock.patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
                              "KAFKA_CONSUMER_CONF": CONSUEMR_CONF,
                              "KAFKA_CONSUMER_GROUP_ID": GROUP_ID,
                              "KAFKA_TOPIC": KAFKA_TOPIC,
                              "KAFKA_TIMEOUT_INTERVAL":
                                  str(KAFKA_TIMEOUT_INTERVAL),
                              "KAFKA_POLLING_INTERVAL":
                                  str(KAFKA_POLLING_INTERVAL),
                              "KAFKA_EXPORT_PATH": "some_path",
                              "KAFKA_EXPORT_BUCKET": "a_bucket",
                              "KAFKA_TS_COL_NM": "ts_column",
                              "KAFKA_PARTITION_KEY_NM": 'partition_column'})
def test__KafkaProducer_should_successfully_connect_to_kafka_broker(mocker):
    class StubbedProducer:
        def poll(self, interval):
            return True

    mocker.patch.object(kafka, "create_producer")
    kafka.create_producer.return_value = StubbedProducer()

    kafka_producer = kafka.KafkaProducer()
    kafka_producer.instantiate_producer()

    assert kafka_producer.producer is not None


@mock.patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
                              "KAFKA_CONSUMER_CONF": CONSUEMR_CONF,
                              "KAFKA_CONSUMER_GROUP_ID": GROUP_ID,
                              "KAFKA_TOPIC": KAFKA_TOPIC,
                              "KAFKA_TIMEOUT_INTERVAL":
                                  str(KAFKA_TIMEOUT_INTERVAL),
                              "KAFKA_POLLING_INTERVAL":
                                  str(KAFKA_POLLING_INTERVAL),
                              "KAFKA_EXPORT_PATH": "some_path",
                              "KAFKA_EXPORT_BUCKET": "a_bucket",
                              "KAFKA_TS_COL_NM": "ts_column",
                              "KAFKA_PARTITION_KEY_NM": 'partition_column'})
def test__KafkaProducer_should_successfully_connect_to_kafka_broker(mocker):
    class StubbedProducerWithNoConnection:
        def poll(self, interval):
            raise Exception

    mocker.patch.object(kafka, "create_producer")
    kafka.create_producer.return_value = StubbedProducerWithNoConnection()
    with pytest.raises(kafka.NoProducerInstantiatedError) as excinfo:
        kafka_producer = kafka.KafkaProducer(
            raise_exception_on_failed_connection=True)
        kafka_producer.instantiate_producer()
    assert excinfo.typename == "NoProducerInstantiatedError"


@mock.patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
                              "KAFKA_CONSUMER_CONF": CONSUEMR_CONF,
                              "KAFKA_CONSUMER_GROUP_ID": GROUP_ID,
                              "KAFKA_TOPIC": KAFKA_TOPIC,
                              "KAFKA_TIMEOUT_INTERVAL":
                                  str(KAFKA_TIMEOUT_INTERVAL),
                              "KAFKA_POLLING_INTERVAL":
                                  str(KAFKA_POLLING_INTERVAL),
                              "KAFKA_EXPORT_PATH": "some_path",
                              "KAFKA_EXPORT_BUCKET": "a_bucket",
                              "KAFKA_TS_COL_NM": "ts_column",
                              "KAFKA_PARTITION_KEY_NM": 'partition_column'})
def test__KafkaProducer_should_successfully_publish_dict_as_msg(mocker):
    class StubbedProducer:
        def produce(self, msg):
            return None

    in_dict = {'someKey': 'some value'}

    mocker.patch.object(kafka, "create_producer")
    kafka.create_producer.return_value = StubbedProducer()
    kafka_producer = kafka.KafkaProducer()
    published_dict = kafka_producer.publish_dict_as_json(in_dict)

    assert published_dict == '{"someKey": "some value"}'


@mock.patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
                              "KAFKA_CONSUMER_CONF": CONSUEMR_CONF,
                              "KAFKA_CONSUMER_GROUP_ID": GROUP_ID,
                              "KAFKA_TOPIC": KAFKA_TOPIC,
                              "KAFKA_TIMEOUT_INTERVAL":
                                  str(KAFKA_TIMEOUT_INTERVAL),
                              "KAFKA_POLLING_INTERVAL":
                                  str(KAFKA_POLLING_INTERVAL),
                              "KAFKA_EXPORT_PATH": "some_path",
                              "KAFKA_EXPORT_BUCKET": "a_bucket",
                              "KAFKA_TS_COL_NM": "ts_column",
                              "KAFKA_PARTITION_KEY_NM": 'partition_column'})
def test__KafkaProducer_should_raise_an_exception_if_it_cant_publish(mocker):
    in_dict = {'someKey': 'some value'}

    class StubbedProducerWithNoConnection:
        def poll(self, interval):
            raise Exception

    mocker.patch.object(kafka, "create_producer")
    kafka.create_producer.return_value = StubbedProducerWithNoConnection()

    with pytest.raises(kafka.NoProducerInstantiatedError) as excinfo:
        kafka_producer = kafka.KafkaProducer(
            raise_exception_on_failed_connection=True)
        published_dict = kafka_producer.publish_dict_as_json(in_dict)

    assert excinfo.typename == "NoProducerInstantiatedError"


def test__KafkaPoller_should_successfully_collect_msgs_from_broker(mocker):
    import queue
    message_queue = queue.Queue(maxsize=3)

    class StubbedMessage:

        def __init__(self, message_body, error):
            self.message_body = message_body
            self.error_on_this_msg = error

        def error(self):
            return self.error_on_this_msg

        def value(self):
            return self.message_body.encode()

    class StubbedConsumer:
        msgs = [None,
                StubbedMessage("message 1", False),
                StubbedMessage("message 2", False),
                StubbedMessage("message 3", False)]

        def poll(self, timeout):
            return self.msgs.pop()

        def subscribe(self, topic):
            return None

    kafka_poller = kafka.KafkaPoller(StubbedConsumer(), 'some_topic', 10)
    returned_messages = kafka_poller.get_msgs()

    assert len(returned_messages) == 3


def test__KafkaPoller_should_successfully_log_msg_errors(mocker):
    import queue
    message_queue = queue.Queue(maxsize=3)

    class StubbedMessage:

        def __init__(self, message_body, error):
            self.message_body = message_body
            self.error_on_this_msg = error

        def error(self):
            return self.error_on_this_msg

        def value(self):
            return self.message_body.encode()

    class StubbedConsumer:
        msgs = [None,
                StubbedMessage("message 1", False),
                StubbedMessage("message 2", "Some error"),
                StubbedMessage("message 3", False)]

        def poll(self, timeout):
            return self.msgs.pop()

        def subscribe(self, topic):
            return None

    kafka_poller = kafka.KafkaPoller(StubbedConsumer(), 'some_topic', 10)
    returned_messages = kafka_poller.get_msgs()

    assert len(returned_messages) == 2


@mock_s3
@freeze_time("2017-03-28 11:10:10")
@mock.patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
                              "KAFKA_CONSUMER_CONF": CONSUEMR_CONF,
                              "KAFKA_CONSUMER_GROUP_ID": GROUP_ID,
                              "KAFKA_TOPIC": KAFKA_TOPIC,
                              "KAFKA_TIMEOUT_INTERVAL":
                                  str(KAFKA_TIMEOUT_INTERVAL),
                              "KAFKA_POLLING_INTERVAL":
                                  str(KAFKA_POLLING_INTERVAL),
                              "KAFKA_EXPORT_PATH": "some_path",
                              "KAFKA_EXPORT_BUCKET": "a_bucket",
                              "KAFKA_TS_COL_NM": "kafka_ts",
                              "KAFKA_PARTITION_KEY_NM": 'partition_column'})
def test__KafkaS3BatchExporter_should_partition_msgs_and_locations(mocker):
    list_of_dicts = [
        {'kafka_ts': "1490198805000",
         'other_field': 'another_field'},
        {'kafka_ts': "1490698805000",
         'other_field': 'another_field'},
        {'kafka_ts': "1490198805000",
         'other_field': 'another_field'}
    ]
    interval = 10
    batch_uploader = kafka.create_batch_s3_uploader()
    list_of_dfs_and_locs = \
        batch_uploader.partition_msgs_by_kafka_ts(list_of_dicts, interval)

    assert len(list_of_dfs_and_locs) == 2
    assert list_of_dfs_and_locs[0][0].shape == (2, 3)
    assert list_of_dfs_and_locs[1][0].shape == (1, 3)
    assert list_of_dfs_and_locs[0][1] \
           == 'some_path/date_of_batch=20170322/time_of_batch=160650/' \
              'df_dt_of_upload_20170328_000000.parquet'
