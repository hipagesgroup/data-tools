"""
Module to connect with and manipulate athena in a pythonic way
"""

import csv
import sys
import time

from hip_data_tools.common import LOG


class AthenaUtil:
    """
    Utility class for connecting to athena and manipulate data in a pythonic way

    Args:
        database: the athena database to run queries on
        conn: AwsConnection object
        output_key: the s3 key where the results of athena queries will be stored
        output_bucket: the s3 bucket where the results of athena queries will be stored
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

    def run_query(self, query_string, return_result=False):
        """
        General purpose query executor that submits an athena query, then uses the execution id
        to poll and monitor the
        sucess of the query. and optionally return the result.
        Args:
            query_string: The string contianing valid athena query
            return_result: Boolean flag to turn on results

        Returns: if return_result = True then returns result dictionary, else None

        """
        athena = self.__get_athena_client()
        output_location = "s3://{bucket}/{key}".format(bucket=self.output_bucket,
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
        stats = self.__watch_query(execution_id)
        LOG.info("athena response %s", response)
        if stats['QueryExecution']['Status']['State'] == 'SUCCEEDED':
            LOG.info("SUCCEEDED")
            if return_result:
                return self.__get_query_result(execution_id)
        else:
            raise ValueError("Query exited with {} state because {}".format(
                stats['QueryExecution']['Status']['State'],
                stats['QueryExecution']['Status']['StateChangeReason']))
        return None

    def __get_athena_client(self):
        return self.conn.get_client(client_type='athena')

    def __watch_query(self, execution_id):
        LOG.info("Watching query with execution id - %s", execution_id)
        while True:
            athena = self.__get_athena_client()
            stats = athena.get_query_execution(QueryExecutionId=execution_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                LOG.info("Query Completed %s", stats)
                return stats
            time.sleep(10)  # 10sec

    def __show_result(self, execution_id, max_result_size=1000):
        results = self.__get_query_result(execution_id, max_result_size)
        headers = [h['Name'].encode("utf-8") for h in
                   results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        LOG.info(headers)
        csv_writer = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
        csv_writer.writerows(
            [[val['VarCharValue'] for val in row['Data']] for row in results['ResultSet']['Rows']])

    def __get_query_result(self, execution_id, max_result_size=1000):
        athena = self.__get_athena_client()
        results = athena.get_query_results(QueryExecutionId=execution_id,
                                           MaxResults=max_result_size)
        # TODO: Add ability to parse pages larger than 1000 rows
        return results

    def repair_table_partitions(self, table):
        """
        Runs repair on given table
        Args:
            table: name of the table whose partitions need to be scanned and refilled

        Returns: None

        """
        self.run_query("MSCK REPAIR TABLE {}".format(table))

    def add_partitions(self, table, partition_keys, partition_values):
        """
        Add a new partition to a given table
        Args:
            table: name of the table to which a new partition is added
            partition_keys: an array of the keys/partition columns
            partition_values: an array of values for partitions

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
        exists = ""
        if table_settings["exists"]:
            exists = "IF NOT EXISTS"
        partitions = ""
        if table_settings["partitions"]:
            partitions = """
            PARTITIONED BY ( 
              {columns}
              )
              """.format(columns=zip_columns(table_settings["partitions"]))

        if table_settings["storage_format_selector"].lower() == "csv" \
            and table_settings["skip_headers"]:
            no_of_skip_lines = 1
            table_properties = """
                TBLPROPERTIES ('has_encrypted_data'='{encryption}', 
                'skip.header.line.count'='{no_of_lines}')
                """.format(encryption=str(table_settings["encryption"]).lower(),
                           no_of_lines=no_of_skip_lines)
        else:
            table_properties = """
                TBLPROPERTIES ('has_encrypted_data'='{encryption}')
                """.format(encryption=str(table_settings["encryption"]).lower())

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
            table_settings: Dictionary of settings to create table

        Returns: None

        """
        self.run_query(self._build_create_table_sql(table_settings))

    def get_table_ddl(self, table):
        """
        Retrive the table DDL in string
        Args:
            table: name of the table for which ddl needs to be generated

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
            table_name: name of the table to be dropped

        Returns: None

        """
        self.run_query("""DROP TABLE IF EXISTS {}""".format(table_name))


def generate_csv_ctas(select_query, destination_table, destination_bucket, destination_key):
    """
    Method to generate a CTAS query string for creating csv output

    Args:
        select_query: the query to be used for table generation
        destination_table: name of the new table being created
        destination_bucket: the s3 bucket where the data from select query will be stored
        destination_key: the s3 directory where the data from select query will be stored

    Returns: CTAS Query in a string

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
        column_list: an array of dictionaries with keys column and type

    Returns: a string containing comma separated list of column name and data type

    """
    return ", ".join(["{} {}".format(col['column'], col["type"]) for col in column_list])


def generate_parquet_ctas(select_query, destination_table, destination_bucket, destination_key):
    """
    Method to generate a CTAS query string for creating parquet output

    Args:
        select_query: the query to be used for table generation
        destination_table: name of the new table being created
        destination_bucket: the s3 bucket where the data from select query will be stored
        destination_key: the s3 directory where the data from select query will be stored

    Returns: CTAS Query in a string

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
