"""
This module provides a simple wrapper to enable the instantiation of kafka
producers and consumers
"""

import json
import os
import time
import traceback
import uuid
from ast import literal_eval
from datetime import datetime

import pandas as pd
from confluent_kafka import Producer, Consumer

# TODO: For the kafka consumer implement the commit of the messages only after
#   successful upload to s3. This will reduce the risks of data loss if the pod
#   has errors after reading the data from Kafka
from hip_data_tools.aws.common import AwsConnectionManager, AwsConnectionSettings, AwsSecretsManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.common import get_from_env_or_default_with_warning, LOG

DEFAULT_PRODUCER_CONF = \
    """{'queue.buffering.max.messages': 10000,
    'queue.buffering.max.ms' : 1000}"""
"""Default configurations for the Kafka producer, as string representing a
python dictionary of configurations"""

DEFAULT_CONSUMER_CONF = \
    """{'auto.offset.reset': 'earliest'}"""
"""
Default configurations for the Kafka consumer, as string representing a
python dictionary of configurations
"""

DEFAULT_KAFKA_TIMESTAMP_COLUMN_NAME = 'kafka_timestamp'
"""
Default name of the column in which we store a message's timestamp
provided by Kafka 
"""

DEFAULT_TIMESTAMP_PARTITION_KEY = 'partition_key_ts'
"""
Default column name for the partitioning message batches uploaded to S3
"""


class ProducerConfig:
    """
    Encapsulation of the kafka configurations, reading from environment if
    not supplied, and defaulting where necessary

    Args:
        bootstrap_servers (string): address and port of bootstrap server
        conf (string(dictionary)) : string of dictionary of configurations

    Attributes:
        configs (dict): Dictionary of the kafka configurations

    """

    def __init__(self,
                 bootstrap_servers=None,
                 conf=None):
        self._bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS") \
            if bootstrap_servers is None else bootstrap_servers

        self.configs = literal_eval(get_from_env_or_default_with_warning(
            "KAFKA_PRODUCER_CONF", DEFAULT_PRODUCER_CONF)) if conf is None \
            else conf

        self.configs.update({'bootstrap.servers': self._bootstrap_servers})


def create_producer(kafka_producer_config=None):
    """
    Creates a Kafka producer

    Args:
       kafka_producer_config (ProducerConfig) : Encapsulated
       configurations for the KafkaProducer
    Returns (Producer): Instantiated Kafka Producer for a specified topic
    """

    kafka_producer_config = kafka_producer_config \
        if kafka_producer_config is not None else ProducerConfig()

    return Producer(kafka_producer_config.configs)


class ConsumerConfig:
    """
    Enscapsulation of the Kafka Consumer Configurations

    Args:
        bootstrap_servers (string): address and port of bootstrap server
        conf (string(dictionary)) : string of dictionary of configurations
        group_id (string): Group Id for the consumer

    Attributes:
        configs (dict): Dictionary of the kafka configurations

    """

    def __init__(self,
                 bootstrap_servers=None,
                 conf=None,
                 group_id=None):
        self.configs = \
            literal_eval(get_from_env_or_default_with_warning(
                "KAFKA_CONSUMER_CONF", DEFAULT_CONSUMER_CONF)) \
                if conf is None else conf

        self._bootstrap_servers = os.environ['KAFKA_BOOTSTRAP_SERVERS'] if \
            bootstrap_servers is None else bootstrap_servers

        self.configs.update({'bootstrap.servers': self._bootstrap_servers})

        # To prevent any clashes with other consumers, if no group id is
        # provided we default to a random uuid
        self._group_id = group_id \
            if group_id is not None else \
            os.environ.get(
                "KAFKA_CONSUMER_GROUP_ID", "random_id_" + str(uuid.uuid4()))

        self.configs.update({'group.id': self._group_id})


def create_consumer(kafka_consumer_config=None):
    """
    Creates a Kafka consumer

    Args:
        kafka_consumer_config (ConsumerConfig): Instantiated kafka
            configuration object
    Returns: Instantiated Kafka Consumer
    """

    kafka_consumer_config = \
        kafka_consumer_config \
            if kafka_consumer_config is not None else ConsumerConfig()

    return Consumer(kafka_consumer_config.configs)


class KafkaPollerConfig:
    """
    Configuration for the poller class

    Args:
        consumer (Consumer): Consumer configurations object
        topic (str): The topic to poll messages from
        timeout_interval (int): Timeout interval for reading the messages
    """

    def __init__(self,
                 consumer=None,
                 topic=None,
                 timeout_interval=None):
        self.consumer = \
            consumer if consumer is not None else create_consumer()

        self.topic = topic if topic is not None else os.environ['KAFKA_TOPIC']
        self.timeout_interval = timeout_interval if timeout_interval is not \
                                                    None \
            else int(os.environ['KAFKA_TIMEOUT_INTERVAL'])


def create_poller(kafka_poller_conf=None):
    """
    Factory method to create a Kafka Poller and take values from environment
    if they aren't provided

    Args:
        kafka_poller_conf (KafkaPollerConfig): Kafka Consumer
    Returns (KafkaPoller): Instantiated KafkaPoller

    """

    poller_config = kafka_poller_conf if kafka_poller_conf is not None \
        else KafkaPollerConfig()

    kafka_poller = KafkaPoller(poller_config.consumer,
                               poller_config.topic,
                               poller_config.timeout_interval)

    return kafka_poller


class BatchS3UploaderConfig:
    """
    Configurations for the BatchS3Uploader

    Args:
        export_path (str): base path within the s3 bucket to export to
        bucket (str): name of the bucket to export results to
        ts_col_nm (str): name given to the time stamp column name
        partition_key_nm (str): name of the column used to store the temporal
            partitioning keys

    Attributes:
        export_path (str): base path within the s3 bucket to export to
        bucket (str): name of the bucket to export results to
        ts_col_nm (str): name given to the time stamp column name
        partition_key_nm (str): name of the column used to store the temporal
            partitioning keys
    """

    def __init__(self,
                 export_path=None,
                 bucket=None,
                 ts_col_nm=None,
                 partition_key_nm=None):
        self.root_path = export_path if export_path is not None \
            else os.environ["KAFKA_EXPORT_PATH"]

        self.bucket = bucket if bucket is not None \
            else os.environ["KAFKA_EXPORT_BUCKET"]

        self.ts_col_nm = ts_col_nm if ts_col_nm is not None else \
            os.environ["KAFKA_TS_COL_NM"]

        self.partition_key_nm = partition_key_nm \
            if partition_key_nm is not None \
            else os.environ["KAFKA_PARTITION_KEY_NM"]


def create_batch_s3_uploader(batch_s3_uploader_config=None):
    """
    Factory method to generate a kafka batch uploadxr

    Args:
        batch_s3_uploader_config (BatchS3UploaderConfig): Configuration
            object for the s3 uploader
    Returns (KafkaS3BatchExporter): Instantiated Exporter

    """

    batch_s3_uploader_config = batch_s3_uploader_config \
        if batch_s3_uploader_config is not None else BatchS3UploaderConfig()

    aws_region = os.environ.get("AWS_DEFAULT_REGION")

    conn = AwsConnectionManager(
        AwsConnectionSettings(region=aws_region, secrets_manager=AwsSecretsManager(), profile=None))

    s3_client = S3Util(conn, batch_s3_uploader_config.bucket)

    return KafkaS3BatchExporter(batch_s3_uploader_config.root_path,
                                s3_client,
                                batch_s3_uploader_config.ts_col_nm,
                                batch_s3_uploader_config.partition_key_nm)


def create_conduit(kafka_poller=None,
                   kafka_exporter=None,
                   polling_interval=None):
    """
    Factory method to create Kafka S3 Conduit, this polls the Kafka queue
    after a set interval and deposits the results onto s3

    Args:
        kafka_poller (KafkaPoller): Kafka Poller used to poll results from a
            topic at set intervals
        kafka_exporter (KafkaS3BatchExporter): Batch Exporter
        polling_interval (int): Time in seconds between polling events

    Returns (KafkaS3Conduit): Instantiated KafkaS3Conduit

    """

    kafka_poller = kafka_poller if kafka_poller is not None else create_poller()
    kafka_exporter = kafka_exporter if kafka_exporter is not None else \
        create_batch_s3_uploader()

    polling_interval = polling_interval if polling_interval is not None else \
        os.environ.get(
            "KAFKA_POLLING_INTERVAL")

    return KafkaConduit(kafka_poller,
                        kafka_exporter,
                        polling_interval)


def generate_snapshot_file_name_with_timestamp():
    """
    Generates the name of the snapshot folder using both the current time
        and date

    Returns (str): String containing the date and time stamped path

    """
    current_datetime = datetime.today().strftime('%Y%m%d_%H%M%S')

    return "df_dt_of_upload_{}" \
        .format(current_datetime)


def add_interval_partitioning_column(msgs_df,
                                     time_column,
                                     partitioning_key,
                                     interval):
    """
    Adding column to a dataframe with a timestamp column rounded to a time
    interval.

    Args:
        msgs_df (DataFrame): Pandas dataframe to with at least 1 timestamp
            column
        time_column (str): Name of the timestamp column to be rounded
        partitioning_key (str): Name of the column in which we create the
        rounded timestamps
        interval (int): Time interval (in seconds) to round timestamps to

    Returns (DataFame): Returns a dataframe with an appended column of
    rounded timestamps
    """

    msgs_df[time_column] = pd.to_datetime(msgs_df[time_column], unit='ms')
    rounding_time_frame = '{}S'.format(int(interval))
    msgs_df[partitioning_key] = msgs_df[time_column].dt.ceil(
        rounding_time_frame)

    return msgs_df


def convert_msgs_to_dictionary(list_of_msgs):
    """
    Converts json msgs into dictionaries, catching any badly formatted into
    string into their own list

    Args:
        list_of_msgs (list(Message Object)): list of json strings

    Returns (list(dict), list(dict)): Returns correctly formatted dictionaries
        in one list and any ill formatted strings in a second list

    """
    good_data = []
    bad_data = []
    induce_warning = False
    for msg in list_of_msgs:
        msg_body = msg.value().decode('utf-8')
        msg_timestamp = msg.timestamp()[1]
        try:
            dict_of_msg = json.loads(msg_body)
            dict_of_msg[DEFAULT_KAFKA_TIMESTAMP_COLUMN_NAME] = msg_timestamp
            good_data.append(dict_of_msg)
        except Exception as ex:
            induce_warning = True
            bad_data_dict = {DEFAULT_KAFKA_TIMESTAMP_COLUMN_NAME: msg_timestamp,
                             'msg_body': msg_body,
                             'error': type(ex).__name__,
                             'error_arguments': ex.args,
                             'error_traceback': traceback.format_exc()}

            bad_data.append(bad_data_dict)

    if induce_warning:
        LOG.warning("Json decoding error, check bad bucket")

    return good_data, bad_data


class KafkaProducer:
    """
    This class provide a wrapper around the lower level Kafka produce,
    and extends this base functionality with commonly used methods or
    approaches to dealing with a message queue


    Args:
        topic (str): Name of Kafka topic to publish the message to
        raise_exception_on_failed_connection (bool): raise exception if the
            Kafka broker cannot be polled
        bootstrap_servers:  (str): String list of bootstrap servers and their
            ports e.g. 'localhost:9092' or '[broker1:9092, broker2:9092]'
        config (str):string  representation of a python dictionary which
            encapsulates the required settings for the consumer

    Attributes:
        topic (str): Name of Kafka topic to publish the message to
        bootstrap_servers:  (str): String list of bootstrap servers and their
            ports e.g. 'localhost:9092' or '[broker1:9092, broker2:9092]'
        config (str):string  representation of a python dictionary which
            encapsulates the required settings for the consumer
        producer (Producer): Kafka producer class

    """

    def __init__(self,
                 topic=None,
                 raise_exception_on_failed_connection=False,
                 bootstrap_servers=None,
                 config=None):

        self.topic = os.environ.get("KAFKA_TOPIC") if topic is None else topic
        self.producer_config = ProducerConfig(bootstrap_servers, config)
        self.producer = None
        self.raise_exception_on_failed_connection = \
            raise_exception_on_failed_connection

    def __del__(self):
        """On destruction flush the local queue to ensure all messages are
        actually pushed to the relevant topic

        Returns : None
        """

        if self.producer is not None:
            self.producer.flush(1)

    def instantiate_producer(self):
        """
        Try to connect to the Kafka bootstrap server. We include
        functionality to allow failure to connect to the queue to happen
        silently

        Returns (Producer): Kafka Producer

        """
        try:
            LOG.debug("Instantiating Producer")
            self.producer = create_producer(self.producer_config)
            # Check the connection works by polling for messages
            LOG.debug("Polling Queue")
            self.producer.poll(3)
            LOG.info("Succesfully polled Kafka Queue")

        except Exception as exception:
            self.producer = None
            LOG.error("Kafka Producer failed to instantiate: \n %s", exception)

            if self.raise_exception_on_failed_connection:
                raise NoProducerInstantiatedError()

    def _instantiated_producer_if_required(self):
        if self.producer is None:
            self.instantiate_producer()

    def publish_dict_as_json(self, in_dict):
        """
        Converts a dictionary to json and pushes it to the Kafka topic

        Args:
            in_dict (dict): Dictionary to be pushed to the topic

        Returns (string): Message published to kafka

        """
        json_string = json.dumps(in_dict)
        self.produce_msg(json_string)
        return json_string

    def produce_msg(self, msg):
        """
        Produces a message to the Kafka topic

        Args:
            msg (str): String to be push to the Kafka topic
        Returns: None

        """
        self._instantiated_producer_if_required()

        if self.producer is not None:
            LOG.debug("Producing message on topic %s : %s", self.topic, msg)
            self.producer.produce(self.topic, msg)
        else:
            LOG.warning(
                "Kafka Connection not initialised, message not sent, "
                "message body: %s", msg)

            if self.raise_exception_on_failed_connection:
                raise NoProducerInstantiatedError


class KafkaPoller:
    """
    This class provides a wrapper around the Kafka consumer to enable polling
    of a kafka queue and aggregation of all messages into a single list

    Args:
        kafka_consumer (Consumer): Kafka Consumer
        topic (str): Topic to subscribe to
        timeout_interval (int): Time int seconds before the consumer times out

    Attributes:
        kafka_consumer (Consumer): Kafka Consumer
        topic (str): Topic to subscribe to
        timeout_interval (int): Time int seconds before the consumer times out
    """

    def __init__(self,
                 kafka_consumer,
                 topic,
                 timeout_interval):

        self._kafka_consumer = kafka_consumer
        self._topic = topic
        self._timeout_interval = timeout_interval
        self._subscribed_to_topic = False

    def _subscribe_consumer(self):
        """
        Subscribe the Kafka consumer to the given topic

        Returns: None

        """
        self._kafka_consumer.subscribe([self._topic])
        self._subscribed_to_topic = True

    def get_msgs(self):
        """Get the latest messages from the Kafka topic

        Returns list(Message) : list of Kafka Messages
        """

        LOG.debug("Getting messages from topic %s", self._topic)

        if not self._subscribed_to_topic:
            self._subscribe_consumer()

        return self.poll_kafka_for_messages()

    def poll_kafka_for_messages(self):
        """
        Poll the kafka topic for messages

        Returns list(Message): list of Kafka messages

        """
        list_of_mgs = []
        while True:
            msg = self._kafka_consumer.poll(self._timeout_interval)

            if msg is None:
                break

            msg = self._check_msg_is_not_error(msg)

            if msg is not None:
                list_of_mgs.append(msg)

        return list_of_mgs

    @staticmethod
    def _check_msg_is_not_error(msg):

        if msg.error():
            LOG.error("Consumer error: %s", msg.error())
            return None

        LOG.debug(
            "Message from topic: %s", msg.value().decode('utf-8'))

        return msg


class KafkaS3BatchExporter:
    """
    This class enables the deposition of a batch of Kafka messages into a s3
    bucket at set intervals
    """

    def __init__(self,
                 root_path,
                 s3_client,
                 ts_col_nm,
                 partition_key_nm):

        self.root_path = root_path
        self.ts_col_nm = ts_col_nm
        self.partition_key_nm = partition_key_nm
        self._s3_client = s3_client

    def parse_and_export_msgs(self,
                              list_of_msgs,
                              interval):
        """
        Converts messages to a pandas dataframe and then exports to s3

        Args:
            list_of_msgs (list(Kafka Message Object)): List of msg objects
            interval (int): Rounding interval for the temporal partitioning
        Returns: None

        """

        good_data, bad_data = convert_msgs_to_dictionary(list_of_msgs)

        self._partition_data_and_upload_to_s3(good_data, interval)
        self._partition_data_and_upload_to_s3(bad_data, interval)

        LOG.info("Data Upload Complete")

    def _partition_data_and_upload_to_s3(self, data_list, interval):
        """
        Partitions the messages by time in a dataframe, and then uploads to s3

        """
        if data_list:

            for msg_df, key, file_name in \
                    self.partition_msgs_by_kafka_ts(data_list, interval):
                LOG.debug("data path : %s/%s", key, file_name)

                self._s3_client.upload_dataframe_as_parquet(dataframe=msg_df, key=key,
                                                            file_name=file_name)

    def partition_msgs_by_kafka_ts(self, list_of_dicts, interval):
        """
        Partition messages by their Kafka timestamps and uses these
        timestamps to generate their relevant paths on s3
        Args:
            list_of_dicts (list(dict)): list of dictionaries to upload to s3
            interval (int): Rounding interval for the temporal partitioning
        Returns list((DataFrame, string, string)): Returns the dataframe for each
        temporal partition, the path to upload it to and file name

        """
        df_msgs_and_meta_data = pd.DataFrame(list_of_dicts)

        df_msgs_and_meta_data = \
            add_interval_partitioning_column(df_msgs_and_meta_data,
                                             self.ts_col_nm,
                                             self.partition_key_nm,
                                             int(interval))
        data_frames_and_fld_locs = self.generate_partitioned_dataframes(
            df_msgs_and_meta_data)

        return data_frames_and_fld_locs

    def generate_partitioned_dataframes(self, df_msgs_and_meta_data):
        """
        Generates the batched dataframes to upload to s3
        Args:
            df_msgs_and_meta_data (dataframe): dataframe to be paritioned
        Returns (List[DataFrame, str, str]): list of triples (df, s3_dir, file_name)
        """
        dataframes_and_fld_locs = []
        for cur_interval_np_datetime in \
                pd.unique(df_msgs_and_meta_data[self.partition_key_nm]):
            df_partition = \
                df_msgs_and_meta_data[
                    df_msgs_and_meta_data[self.partition_key_nm] ==
                    cur_interval_np_datetime]

            cur_interval_ts = pd.to_datetime(cur_interval_np_datetime)

            batch_file_path = "{}/date_of_batch={}/time_of_batch={}".format(
                self.root_path,
                cur_interval_ts.strftime('%Y%m%d'),
                cur_interval_ts.strftime('%H%M%S')
            )

            file_nm = generate_snapshot_file_name_with_timestamp()

            LOG.debug("Data path : %s", batch_file_path)

            dataframes_and_fld_locs.append((df_partition, batch_file_path, file_nm))

        return dataframes_and_fld_locs


class KafkaConduit:
    """
    This class polls the Kafka topic at set intervals and exports the results
    to an s3 bucket

    Args:
        kafka_poller (KafkaPoller): Instantiated Kafka Poller
        kafka_exporter (KafkaS3BatchExporter): Exporter to parse and export
            Kafka messages
        Polling Interval (int): Interval in Seconds at which to poll the
            Kafka topic and export the messages to s3
    """

    def __init__(self,
                 kafka_poller,
                 kafka_exporter,
                 polling_interval):
        self._kafka_poller = kafka_poller
        self._kafka_s3_exporter = kafka_exporter
        self._polling_interval = polling_interval

    def poll_topic_and_upload_to_s3(self):
        """
        Poll at the Kafka topic at set intervals and parse and export the
        messages to S3
        Returns: None
        """
        while True:
            LOG.debug("Polling Kafka for messages")
            self.create_events_snapshot()
            LOG.debug("Upload complete, sleeping")
            time.sleep(self._polling_interval)

    def create_events_snapshot(self):
        """
        Get Kafka messages from a topic and export to s3

        Returns: None

        """

        msgs = self._kafka_poller.get_msgs()
        LOG.debug("Json messages : %s", msgs)

        self._kafka_s3_exporter.parse_and_export_msgs(msgs,
                                                      self._polling_interval)


class NoProducerInstantiatedError(Exception):
    """Exception raised when no producer has been instantiated"""
