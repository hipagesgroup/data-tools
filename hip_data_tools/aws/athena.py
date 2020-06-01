"""
Utility to connect to, and perform DML and DDL operations on aws Athena
"""

import csv
import sys
import time
from typing import List, Any, Tuple

from pandas import DataFrame

from hip_data_tools.aws.common import AwsUtil, AwsConnectionManager
from hip_data_tools.common import LOG

_PYTHON_TO_ATHENA_DATA_TYPE_MAP = {
    "Timestamp": "TIMESTAMP",
    "str": "STRING",
    "int64": "BIGINT",
    "int32": "INT",
    "dict": "MAP",
    "float64": "DOUBLE",
    "UUID": "STRING",
    "object": "STRING",
    "int": "INT",
    "list": "STRING",
    "NoneType": "STRING",
    "bool_": "BOOLEAN",
    "Bool": "BOOLEAN",
    "bool": "BOOLEAN",
}


def get_partitions_from_partitions_dict(partitions: dict):
    """
    Get the Athena table settings partitions list of dictionary
    Args:
        partitions (dict): dictionary of partition column name and value
    :return: List of dictionary
    """
    if partitions is not None:
        column_name = partitions["column"]
        return [{"column": column_name, "type": "STRING"}]
    return None


def extract_athena_type_from_value(val: Any) -> str:
    """
    Extracts athena compatible datatype from a given python object
    Args:
        val (Any): Any value from primitive python types
    Returns: str
    """
    return _PYTHON_TO_ATHENA_DATA_TYPE_MAP[type(val).__name__]


def get_table_settings_for_dataframe(dataframe: DataFrame, partitions: dict, s3_bucket: str,
                                     s3_dir: str, table: str) -> dict:
    """
    Get the Athena table settings
    Args:
        dataframe (DataFrame): data frame with column types and names
        partitions (dict): dictionary of partition column name and value
        s3_bucket (str): S3 bucket name to store Athena table's data
        s3_dir (str): S3 directory to store Athena table's data
        table (str): Name of the table
    :return: table settings
    """
    table_settings = {
        "exists": True,
        "partitions": get_partitions_from_partitions_dict(partitions),
        "storage_format_selector": "parquet",
        "encryption": False,
        "table": table,
        "columns": get_athena_columns_from_dataframe(dataframe),
        "s3_bucket": s3_bucket,
        "s3_dir": s3_dir,
    }
    return table_settings


class AthenaUtil(AwsUtil):
    """
    Utility class for connecting to athena and manipulate data in a pythonic way

    Args:
        database (string): the athena database to run queries on
        conn (AwsConnection): AwsConnection object
        output_key (string): the s3 key where the results of athena queries will be stored
        output_bucket (string): the s3 bucket where the results of athena queries will be stored
        work_group (string): Athena workGroup name
    """

    def __init__(self,
                 database: str,
                 conn: AwsConnectionManager,
                 output_key: str = None,
                 output_bucket: str = None,
                 work_group: str = "primary"):
        super().__init__(conn, "athena")
        self.database = database
        self.conn = conn
        self.output_key = output_key
        self.output_bucket = output_bucket
        self.work_group = work_group
        self.storage_format_lookup = {
            "parquet": {
                "row_format_serde": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "outputformat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "inputformat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
            },
            "csv": {
                "row_format_serde": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                "outputformat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "inputformat": "org.apache.hadoop.mapred.TextInputFormat"
            },
            "json": {
                "row_format_serde": "org.openx.data.jsonserde.JsonSerDe",
                "outputformat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "inputformat": "org.apache.hadoop.mapred.TextInputFormat"
            }
        }

    def run_query(self, query_string, return_result=False):
        """
        General purpose query executor that submits an athena query, then uses the execution id
        to poll and monitor the
        sucess of the query. and optionally return the result.
        Args:
            query_string (string): The string contianing valid athena query
            return_result (boolean): Boolean flag to turn on results

        Returns (boolean): if return_result = True then returns result dictionary, else None

        """
        athena = self.get_client()
        output_location = "s3://{bucket}/{key}".format(
            bucket=self.output_bucket,
            key=self.output_key)
        LOG.info("executing query \n%s \non database - %s with results location %s", query_string,
                 self.database,
                 output_location)
        response = athena.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={
                'Database': self.database
            },
            ResultConfiguration={
                'OutputLocation': output_location
            },
            WorkGroup=self.work_group
        )
        execution_id = response['QueryExecutionId']
        stats = self.watch_query(execution_id)
        LOG.info("athena response %s", response)
        if stats['QueryExecution']['Status']['State'] == 'SUCCEEDED':
            LOG.info("Query execution id - %s SUCCEEDED", execution_id)
            if return_result:
                return self._get_query_result(execution_id)
        else:
            raise ValueError("Query exited with {} state because {}".format(
                stats['QueryExecution']['Status']['State'],
                stats['QueryExecution']['Status']['StateChangeReason']))
        return None

    def watch_query(self, execution_id, poll_frequency=10):
        """
        Watch the query execution for a given execution id in Athena
        Args:
            execution_id: the execution id of an Athena Auery
            poll_frequency (int): Freq in seconds to poll for the query status using Athen API

        Returns: dictionary of status from Athena

        """
        LOG.info("Watching query with execution id - %s", execution_id)
        while True:
            athena = self.get_client()
            stats = athena.get_query_execution(QueryExecutionId=execution_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                LOG.info("Query Completed %s", stats)
                return stats
            time.sleep(poll_frequency)

    def _show_result(self, execution_id, max_result_size=1000):
        results = self._get_query_result(execution_id, max_result_size)
        column_info = results['ResultSet']['ResultSetMetadata']['ColumnInfo']
        headers = [h['Name'].encode('utf-8') for h in column_info]
        LOG.info(headers)
        csv_writer = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
        csv_writer.writerows(
            [[val['VarCharValue'] for val in row['Data']] for row in results['ResultSet']['Rows']])

    def _get_query_result(self, execution_id, max_result_size=1000):
        athena = self.get_client()
        results = athena.get_query_results(QueryExecutionId=execution_id,
                                           MaxResults=max_result_size)
        # TODO: Add ability to parse pages larger than 1000 rows
        return results

    def repair_table_partitions(self, table):
        """
        Runs repair on given table
        Args:
            table (string): name of the table whose partitions need to be scanned and refilled

        Returns: None

        """
        self.run_query("MSCK REPAIR TABLE {}".format(table))

    def add_partitions(self, table, partition_keys, partition_values):
        """
        Add a new partition to a given table
        Args:
            table (string): name of the table to which a new partition is added
            partition_keys (list): an array of the keys/partition columns
            partition_values (list): an array of values for partitions

        Returns: None

        """
        partition_kv = ["{}='{}'".format(key, value) for key, value in
                        zip(partition_keys, partition_values)]
        partition_query = """
        ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION ({partitions});
        """.format(table_name=table,
                   partitions=', '.join(partition_kv))
        self.run_query(query_string=partition_query)

    def _build_create_table_sql(self, table_settings):
        exists = _construct_table_exists_ddl(table_settings["exists"])
        partitions = _construct_table_partition_ddl(table_settings["partitions"])
        table_properties = _construct_table_properties_ddl(
            table_settings.get("skip_headers", False),
            table_settings["storage_format_selector"].lower(),
            table_settings["encryption"])

        sql = """
            CREATE EXTERNAL TABLE {exists} {table}(
              {columns}
              )
            {partitions}
            ROW FORMAT SERDE 
              '{row_format_serde}' 
            STORED AS INPUTFORMAT 
              '{inputformat}' 
            OUTPUTFORMAT 
              '{outputformat}'
            LOCATION
              's3://{s3_bucket}/{s3_dir}'
            {table_properties}
            """.format(table=table_settings["table"],
                       exists=exists,
                       columns=zip_columns(table_settings["columns"]),
                       partitions=partitions,
                       row_format_serde=self.storage_format_lookup[
                           table_settings["storage_format_selector"]]["row_format_serde"],
                       inputformat=self.storage_format_lookup[
                           table_settings["storage_format_selector"]]["inputformat"],
                       outputformat=self.storage_format_lookup[
                           table_settings["storage_format_selector"]]["outputformat"],
                       s3_bucket=table_settings["s3_bucket"],
                       s3_dir=table_settings["s3_dir"],
                       table_properties=table_properties)
        LOG.info("Query from table settings:\n %s", sql)
        return sql

    def create_table(self, table_settings):
        """
        Create a table from given settings
        Args:
            table_settings (dict): Dictionary of settings to create table

        Returns: None
        """
        table_sql = self._build_create_table_sql(table_settings)
        LOG.info(table_sql)
        self.run_query(table_sql)

    def get_table_ddl(self, table):
        """
        Retrive the table DDL in string
        Args:
            table (string): name of the table for which ddl needs to be generated

        Returns: string containing the athena table DDL

        """
        # Read the ddl of temporary table
        ddl_result = self.run_query("""SHOW CREATE TABLE {}""".format(table), return_result=True)
        ddl = ""
        for row in ddl_result["ResultSet"]["Rows"]:
            for column in row["Data"]:
                ddl = ddl + " " + column["VarCharValue"]
            ddl = ddl + "\n"
        return ddl

    def get_table_data_location(self, table: str) -> tuple:
        """
        Retrieves the table's S3 data location using glue meta store
        Args:
            table (str): name of the table
        Returns: tuple of s3 bucket and key
        """
        table = self.get_glue_table_metadata(table)
        location = table['Table']['StorageDescriptor']['Location']
        bucket = location.split("/")[2]
        key = "/".join(location.split("/")[3:])
        return (bucket, key)

    def get_table_columns(self, table: str) -> Tuple[List[dict], List[dict]]:
        """
        Retrieves the table's columns using glue meta store
        Args:
            table (str): name of the table
        Returns: tuple of regular and partition column dictonaries
        """
        table = self.get_glue_table_metadata(table)
        regular_columns = table["Table"]["StorageDescriptor"]["Columns"]
        partition_columns = table["Table"]["PartitionKeys"]
        return (regular_columns, partition_columns)

    def get_glue_table_metadata(self, table: str) -> dict:
        """
        Get table metadata from glue
        Args:
            table (str): Athena table name
        Returns: A dict of table metadata
        """
        return self.conn.client('glue').get_table(DatabaseName=self.database, Name=table)

    def drop_table(self, table_name):
        """
        Drop a given athena table

        Args:
            table_name (string): name of the table to be dropped

        Returns: None

        """
        self.run_query("""DROP TABLE IF EXISTS {}""".format(table_name))


def generate_csv_ctas(select_query, destination_table, destination_bucket, destination_key,
                      partition_fields=''):
    """
    Method to generate a CTAS query string for creating csv output

    Args:
        select_query (string): the query to be used for table generation
        destination_table (string): name of the new table being created
        destination_bucket (string): the s3 bucket where the data from select query will be stored
        destination_key (string): the s3 directory where the data from select query will be stored
        partition_fields (string): partition field names

    Returns (string): CTAS Query in a string

    """
    return _get_ctas_statement(destination_bucket, destination_key, destination_table,
                               partition_fields, select_query, file_format='TEXTFILE')


def generate_parquet_ctas(select_query, destination_table, destination_bucket, destination_key,
                          partition_fields=''):
    """
    Method to generate a CTAS query string for creating parquet output

    Args:
        select_query (string): the query to be used for table generation
        destination_table (string): name of the new table being created
        destination_bucket (string): the s3 bucket where the data from select query will be stored
        destination_key (string): the s3 directory where the data from select query will be stored
        partition_fields (string): partition field names

    Returns (string): CTAS Query in a string

    """
    return _get_ctas_statement(destination_bucket, destination_key, destination_table,
                               partition_fields, select_query, file_format='parquet')


def _get_ctas_statement(destination_bucket, destination_key, destination_table, partition_fields,
                        select_query, file_format):
    delimiter = ''
    if file_format == 'TEXTFILE':
        delimiter = "field_delimiter=',',"
    partitioned_by = ""
    if partition_fields != '':
        partitioned_by = """,
        partitioned_by = ARRAY[{partition_keys}]
        """.format(partition_keys=partition_fields)
    final_query = """
        CREATE TABLE {destination_table}
        WITH (
            {delimiter}
            format='{file_format}',
            external_location='s3://{bucket}/{key}'{partitioned_by}
        ) AS
        {athena_query}
        """.format(destination_table=destination_table,
                   bucket=destination_bucket,
                   key=destination_key,
                   athena_query=select_query,
                   partitioned_by=partitioned_by,
                   file_format=file_format,
                   delimiter=delimiter)
    return final_query


def zip_columns(column_list):
    """
    Combine the column list into a zipped comma separated list of column name and data type
    Args:
        column_list (list): an array of dictionaries with keys column and type

    Returns (string): a string containing comma separated list of column name and data type

    """
    return ", ".join(["{} {}".format(col['column'], col["type"]) for col in column_list])


def _construct_table_partition_ddl(partitions):
    partition_query = ""
    if partitions:
        partition_query = """
        PARTITIONED BY ( 
          {columns}
          )
          """.format(columns=zip_columns(partitions))
    return partition_query


def _construct_table_exists_ddl(enable_exists):
    exists = ""
    if enable_exists:
        exists = "IF NOT EXISTS"
    return exists


def _construct_table_properties_ddl(skip_headers, storage_format_selector, encryption):
    if storage_format_selector == "csv" and skip_headers:
        no_of_skip_lines = 1
        table_properties = """
            TBLPROPERTIES ('has_encrypted_data'='{encryption}', 
            'skip.header.line.count'='{no_of_lines}')
            """.format(encryption=str(encryption).lower(),
                       no_of_lines=no_of_skip_lines)
    else:
        table_properties = """
            TBLPROPERTIES ('has_encrypted_data'='{encryption}')
            """.format(encryption=str(encryption).lower())
    return table_properties


def _get_data_frame_column_types(data_frame):
    data_frame_col_dict = {}
    for col in data_frame:
        data_frame_col_dict[col] = type(data_frame[col][0]).__name__
    return data_frame_col_dict


def get_athena_columns_from_dataframe(data_frame: DataFrame) -> List[dict]:
    """
    Extracts a dictionary of column names and their athena data types from the dataframe
    Args:
        data_frame (DataFrame): the dataframe which the columns need to be extracted
    Returns: list of dict
    """
    column_dtype = _get_data_frame_column_types(data_frame)
    return [
        {"column": field_name, "type": _PYTHON_TO_ATHENA_DATA_TYPE_MAP.get(field_type, "STRING")}
        for
        field_name, field_type in column_dtype.items()]
