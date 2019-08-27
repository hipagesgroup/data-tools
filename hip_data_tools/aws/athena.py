"""
Utility to connect to, and perform DML and DDL operations on aws Athena
"""

import csv
import sys
import time

from hip_data_tools.common import LOG


class AthenaUtil:
    """
    Utility class for connecting to athena and manipulate data in a pythonic way

    Args:
        database (string): the athena database to run queries on
        conn (AwsConnection): AwsConnection object
        output_key (string): the s3 key where the results of athena queries will be stored
        output_bucket (string): the s3 bucket where the results of athena queries will be stored
    """

    def __init__(self, database, conn, output_key=None, output_bucket=None):
        self.database = database
        self.conn = conn
        self.output_key = output_key
        self.output_bucket = output_bucket
        self.storage_format_lookup = {
            "parquet": {
                "row_format_serde": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "outputformat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "inputformat": "org.apache.hadoop.mapred.TextInputFormat"
            },
            "csv": {
                "row_format_serde": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                "outputformat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "inputformat": "org.apache.hadoop.mapred.TextInputFormat"
            }
        }
        self.boto_type = "athema"

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
        athena = self.conn.client(self.boto_type)
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
            }
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
            athena = self.conn.client(self.boto_type)
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
        athena = self.conn.client(self.boto_type)
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
        return sql

    def create_table(self, table_settings):
        """
        Create a table from given settings
        Args:
            table_settings (dict): Dictionary of settings to create table

        Returns: None

        """
        self.run_query(self._build_create_table_sql(table_settings))

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

    def drop_table(self, table_name):
        """
        Drop a given athena table

        Args:
            table_name (string): name of the table to be dropped

        Returns: None

        """
        self.run_query("""DROP TABLE IF EXISTS {}""".format(table_name))


def generate_csv_ctas(select_query, destination_table, destination_bucket, destination_key):
    """
    Method to generate a CTAS query string for creating csv output

    Args:
        select_query (string): the query to be used for table generation
        destination_table (string): name of the new table being created
        destination_bucket (string): the s3 bucket where the data from select query will be stored
        destination_key (string): the s3 directory where the data from select query will be stored

    Returns (string): CTAS Query in a string

    """
    final_query = """
    CREATE TABLE {destination_table}
    WITH (
        field_delimiter='{field_delimiter}',
        format='TEXTFILE',
        external_location='s3://{bucket}/{key}'
    ) AS
    {athena_query}
    """.format(
        field_delimiter=",",
        destination_table=destination_table,
        bucket=destination_bucket,
        key=destination_key,
        athena_query=select_query, )
    return final_query


def zip_columns(column_list):
    """
    Combine the column list into a zipped comma separated list of column name and data type
    Args:
        column_list (list): an array of dictionaries with keys column and type

    Returns (string): a string containing comma separated list of column name and data type

    """
    return ", ".join(["{} {}".format(col['column'], col["type"]) for col in column_list])


def generate_parquet_ctas(select_query, destination_table, destination_bucket, destination_key):
    """
    Method to generate a CTAS query string for creating parquet output

    Args:
        select_query (string): the query to be used for table generation
        destination_table (string): name of the new table being created
        destination_bucket (string): the s3 bucket where the data from select query will be stored
        destination_key (string): the s3 directory where the data from select query will be stored

    Returns (string): CTAS Query in a string

    """
    final_query = """
    CREATE TABLE {destination_table}
    WITH (
        format='parquet',
        external_location='s3://{bucket}/{key}'
    ) AS
    {athena_query}
    """.format(
        destination_table=destination_table,
        bucket=destination_bucket,
        key=destination_key,
        athena_query=select_query, )
    return final_query


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
