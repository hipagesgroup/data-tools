"""
This module provides a simple wrapper to enable the instantiation of kafka
producers and consumers
"""

import json
import logging as log
import os
import time
import traceback
import uuid
from ast import literal_eval
from datetime import datetime

import pandas as pd
from confluent_kafka import Producer, Consumer

from hip_data_tools.authenticate import AwsConnection
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.common import get_from_env_or_default_with_warning

# TODO:
#  1. For the kafka consumer implement the commit of the messages only after
#   successful upload to s3. This will reduce the risks of data loss if the pod
#   has errors after reading the data from Kafka

DEFAULT_PRODUCER_CONF = \
    """{'queue.buffering.max.messages': 10000,
    'queue.buffering.max.ms' : 1000}"""
"""Default configurations for the Kafka producer, as string representing a
python dictionary of configurations"""

DEFAULT_CONSUMER_CONF = \
    """{'auto.offset.reset': 'earliest'}"""
"""Default configurations for the Kafka consumer, as string representing a
python dictionary of configurations"""

DEFAULT_KAFKA_TIMESTAMP_COLUMN_NAME = 'kafka_timestamp'
"""Default name of the column in which we store a message's timestamp
provided by Kafka """

DEFAULT_TIMESTAMP_PARTITION_KEY = 'partition_key_ts'
"""
Default column name for the partitioning message batches uploaded to S3
"""


def create_producer(bootstrap_servers=None,
                    conf=None):
    """
    Creates a Kafka producer
    Args:
        bootstrap_servers (str): String list of bootstrap servers and their
            ports e.g. 'localhost:9092' or '[broker1:9092, broker2:9092]'
        conf (str): A string representation of a python dictionary which
        encapsulates the required settings for the producer
    Returns (Producer): Instantiated Kafka Producer for a specified topic
    """

    bootstrap_servers = bootstrap_servers if bootstrap_servers is not \
                                             None else \
        get_from_env_or_default_with_warning(
            "KAFKA_BOOTSTRAP_SERVERS", 'localhost:9092')

    configs = literal_eval(get_from_env_or_default_with_warning(
        "KAFKA_PRODUCER_CONF", DEFAULT_PRODUCER_CONF)) if conf is None else conf

    configs.update({'bootstrap.servers': bootstrap_servers})

    return Producer(configs)


def create_consumer(bootstrap_servers=None,
                    group_id=None,
                    conf=None):
    """
    Creates a Kafka consumer
    Args:
        bootstrap_servers:  (str): String list of bootstrap servers and their
            ports e.g. 'localhost:9092' or '[broker1:9092, broker2:9092]'
        group_id (str) : Group Id for the consumer
        conf:  string representation of a python dictionary which
        encapsulates the required settings for the consumer

    Returns: Instantiated Kafka Consumer

    """
    bootstrap_servers = bootstrap_servers if bootstrap_servers is not \
                                             None else \
        get_from_env_or_default_with_warning(
            "KAFKA_BOOTSTRAP_SERVERS", 'localhost:9092')

    # To prevent any clashes with other consumers, if no group id is
    # provided we default to a random uuid
    group_id = group_id if group_id is not \
                           None else \
        get_from_env_or_default_with_warning(
            "KAFKA_CONSUMER_GROUP_ID", "random_id_" + str(uuid.uuid4()))

    configs = literal_eval(get_from_env_or_default_with_warning(
        "KAFKA_CONSUMER_CONF", DEFAULT_CONSUMER_CONF)) if conf is None else conf

    configs.update({'bootstrap.servers': bootstrap_servers})
    configs.update({'group.id': group_id})

    return Consumer(configs)


def create_kafka_poller(kafka_consumer=None,
                        topic=None,
                        timeout_interval=None):
    """
    Factory method to create a Kafka Poller and take values from environment
    if they aren't provided
    Args:
        kafka_consumer (Consumer): Kafka Consumer
        topic (str): Name of the topic to subscribe to
        timeout_interval (int): Timeout interval for kafka connection

    Returns (KafkaPoller): Instantiated KafkaPoller

    """

    kafka_consumer = kafka_consumer if kafka_consumer is not None \
        else create_consumer()

    topic = topic if topic is not None else os.environ.get("KAFKA_TOPIC")

    timeout_interval = int(timeout_interval) if timeout_interval is not None \
        else int(os.environ.get("KAFKA_TIMEOUT_INTERVAL"))

    kafka_poller = KafkaPoller(kafka_consumer, topic, timeout_interval)

    return kafka_poller


def create_kafka_batch_s3_uploader(root_path=None,
                                   bucket=None,
                                   ts_col_nm=None,
                                   partition_key_nm=None):
    """
    Factory method to generate a kafka batch uploader
    Args:
        root_path (str): path for data to be exported
        bucket (str): Name of bucket for export
        ts_col_nm (str): Name of the timestamp column used to partition the
            messages
        partition_key_nm (str): Name of the column in which to deposit the
            rounded timestamps used to partition the data
    Returns (KafkaS3BatchExporter): Instantiated Exporter

    """
    root_path = root_path if root_path is not None else os.environ.get(
        "KAFKA_EXPORT_PATH")

    bucket = bucket if bucket is not None else os.environ.get(
        "KAFKA_EXPORT_BUCKET")

    ts_col_nm = ts_col_nm if ts_col_nm is not None else os.environ.get(
        "KAFKA_TS_COL_NM")

    partition_key_nm = partition_key_nm if partition_key_nm is not None else \
        os.environ.get(
            "KAFKA_PARTITION_KEY_NM")

    aws_region = os.environ.get("AWS_DEFAULT_REGION")

    conn = AwsConnection(mode="standard_env_var", region_name=aws_region,
                         settings={})

    s3_client = S3Util(conn, bucket)

    return KafkaS3BatchExporter(root_path,
                                s3_client,
                                ts_col_nm,
                                partition_key_nm)


def create_kafka_conduit(kafka_poller,
                         kafka_exporter,
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

    return "df_dt_of_upload_{}.parquet" \
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
        log.warning("Json decoding error, check bad bucket")

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
        self.bootstrap_servers = bootstrap_servers
        self.config = config
        self.producer = self.instantiate_producer()
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
            log.debug("Instantiating Producer")
            producer = create_producer(self.bootstrap_servers, self.config)
            # Check the connection works by polling for messages
            log.debug("Polling Queue")
            producer.poll(3)
            log.info("Succesfully polled Kafka Queue")

        except Exception as exception:
            producer = None
            log.error("Kafka Producer failed to instantiate: \n %s", exception)

            if self.raise_exception_on_failed_connection:
                raise exception

        return producer

    def publish_dict_as_json(self, in_dict):
        """
        Converts a dictionary to json and pushes it to the Kafka topic
        Args:
            in_dict (dict): Dictionary to be pushed to the topic

        Returns: None

        """
        json_string = json.dumps(in_dict)
        self.produce_msg(json_string)

    def produce_msg(self, msg):
        """
        Produces a message to the Kafka topic
        Args:
            msg (str): String to be push to the Kafka topic
        Returns: None

        """
        if self.producer is not None:
            log.debug("Producing message on topic %s : %s", self.topic, msg)
            self.producer.produce(self.topic, msg)
        else:
            log.warning(
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

    def subscribe_consumer(self):
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

        log.debug("Getting messages from topic %s", self._topic)

        list_of_mgs = []

        if not self._subscribed_to_topic:
            self.subscribe_consumer()

        while True:
            msg = self._kafka_consumer.poll(self._timeout_interval)

            if msg is None:
                break

            if msg.error():
                log.error("Consumer error: %s", msg.error())
                continue
            else:

                log.debug(
                    "Message from topic: %s", msg.value().decode('utf-8'))

                list_of_mgs.append(msg)

        return list_of_mgs


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

        log.info("Data Upload Complete")

    def _partition_data_and_upload_to_s3(self, data_list, interval):
        """
        Partitions the messages by time in a dataframe, and then uploads to s3

        """
        if data_list:

            for msg_df, s3_path in \
                    self.partition_msgs_and_locations(data_list, interval):
                log.debug("data path : %s", s3_path)

                self._s3_client.upload_df_to_s3(msg_df, s3_path)

    def partition_msgs_and_locations(self, list_of_dicts, interval):
        """
        Partition messages by their Kafka timestamps and uses these
        timestamps to generate their relevant paths on s3
        Args:
            list_of_dicts (list(dict)): list of dictionaries to upload to s3
            interval (int): Rounding interval for the temporal partitioning
        Returns list((DataFrame, string)): Returns the dataframe for each
        temporal partition, and the path to upload it to

        """
        df_msgs_and_meta_data = pd.DataFrame(list_of_dicts)

        df_msgs_and_meta_data = \
            add_interval_partitioning_column(df_msgs_and_meta_data,
                                             self.ts_col_nm,
                                             self.partition_key_nm,
                                             int(interval))
        fld_locations = []
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

            full_path_on_s3 = '{}/{}'.format(batch_file_path, file_nm)

            log.debug("Data path : %s", full_path_on_s3)

            fld_locations.append((df_partition, full_path_on_s3))

        return fld_locations


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
            log.debug("Polling Kafka for messages")
            self.create_events_snapshot()
            log.debug("Upload complete, sleeping")
            time.sleep(self._polling_interval)

    def create_events_snapshot(self):
        """
        Get Kafka messages from a topic and export to s3

        Returns: None

        """

        msgs = self._kafka_poller.get_msgs()
        log.debug("Json messages : %s", msgs)

        self._kafka_s3_exporter.parse_and_export_msgs(msgs,
                                                      self._polling_interval)


class NoProducerInstantiatedError(Exception):
    """Exception raised when no producer has been instantiated"""
