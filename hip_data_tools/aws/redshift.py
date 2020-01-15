import re
import boto3
import psycopg2

DTYPE_REDSHIFT_TO_CSV_ATHENA = {
    # Numeric
    "INTEGER": "BIGINT",
    "BIGINT": "BIGINT",
    "DOUBLE": "DOUBLE",
    "SMALLINT": "BIGINT",
    "REAL": "BIGINT",
    "NUMERIC": "DOUBLE",

    # Date & time
    "TIMESTAMP": "STRING",

    # Strings
    "BOOLEAN": "STRING",
    "DATE": "STRING",
    "CHARACTER": "STRING"
}

DTYPE_REDSHIFT_TO_PARQUET_ATHENA = {
    # Numeric
    "INTEGER": "INTEGER",
    "BIGINT": "BIGINT",
    "DOUBLE": "DOUBLE",
    "SMALLINT": "SMALLINT",
    "REAL": "BIGINT",
    "NUMERIC": "DECIMAL",
    "BOOLEAN": "BOOLEAN",
    "CHARACTER": "STRING",

    # Date & time
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE"
}


class RedshiftUtil:
    """
    Utility to read, write and interact with redshift.
    The environments are isolated with schemas.
    there are convenience methods for most types of transformations and interactions.
    """

    def __init__(self, host, port, schema, user, password):
        self.host = host
        self.port = port
        self.schema = schema
        self.user = user
        self.password = password

    def load_table_from_s3(self, s3bucket, s3key, column_list, table,
                           delimiter='|', s3_conn_id="s3_gandalf_write", empty_as_null=True):
        """
        Read all files from the s3 source bucket and key, and load the csv files onto the given table
        Args:
            s3bucket (str): S3 bucket to read files from
            s3key (str): S3 Key prefix under which all files are to be read
            column_list: A List which contains the column names in the same sequence as the fields to be loaded
            table: table to be loaded with csv data
            delimiter: csv delimiter, defaults to '|'
            s3_conn_id: the s3 connection to be used for reading the files defaults to 's3_gandalf_write'
            empty_as_null: Boolean flag to treat empty csv columns as NULL, defaults to True
        Returns: N/A
        """
        # Retrieve aws credentials from conn string
        aws_access_key_id, aws_secret_access_key = self.__retrieve_aws_creds(s3_conn_id)
        empty_handle = "EMPTYASNULL " if empty_as_null else ""
        sql_statement = """
            COPY {schema}.{table} 
            ({column_list}) 
            FROM 's3://{s3bucket}/{s3key}/' 
            DELIMITER '{delimiter}' 
            DATEFORMAT AS 'auto' 
            {empty_handle} 
            ESCAPE 
            TRUNCATECOLUMNS 
            COMPUPDATE OFF
            gzip
            """.format(
            schema=self.schema,
            table=table,
            column_list=column_list,
            s3bucket=s3bucket,
            s3key=s3key,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            delimiter=delimiter,
            empty_handle=empty_handle)
        print(sql_statement)
        sql_statement = sql_statement + """
            CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}';
            """.format(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        # Fill in credentials
        conn, cursor = self.__get_redshift_cursor()
        cursor.execute(sql_statement)
        cursor.close()
        conn.commit()
        print("S3 data loaded")

    def create_temptable(self, table, suffix):
        """
        use the structure of source table and drop create a replica with suffix
        Args:
            table: the table to be duplicated
            suffix: suffix to add to the new table's name
        Returns: None
        """
        # build the SQL statement
        drop_sql_statement = "DROP TABLE IF EXISTS {schema}.{table}_{suffix};".format(
            schema=self.schema, table=table, suffix=suffix)
        print(drop_sql_statement)
        conn, cursor = self.__get_redshift_cursor()
        cursor.execute(drop_sql_statement)
        # build the SQL statement
        create_sql_statement = "CREATE TABLE IF NOT EXISTS {schema}.{table}_{suffix} (like {schema}.{table});".format(
            schema=self.schema, table=table, suffix=suffix)
        print(create_sql_statement)
        cursor.execute(create_sql_statement)
        cursor.close()
        conn.commit()
        print("Temporary table created")

    def execute_query(self, query):
        """
        Executes a query on redshift
        Args:
            query: Valid SQL query in a string
        Returns: returns rows
        """
        print(query)
        rows = None
        conn, cursor = self.__get_redshift_cursor()
        cursor.execute(query)
        if cursor.description:
            rows = cursor.fetchall()
        cursor.close()
        conn.commit()
        print("Query execution completed")
        return rows

    def bulk_upsert(self, source_table, destination_table, unique_keys):
        """
        Upsert all rows from source table to a destination table based on the list of columns provided as unique keys.
        If an empty list is provided for Unique Keys, a table swap is performed.
        Args:
            source_table: table with source data
            destination_table: table which needs to be upserted with data
            unique_keys: A list of columns that are to be used for upsert
        Returns: None
        """
        # build the SQL statement
        sql_statement = "BEGIN;\n"
        # Check if there are no keys, then drop create table
        if len(unique_keys) == 0:
            sql_statement += " DROP TABLE IF EXISTS {schema}.{destination_table};\n".format(
                schema=self.schema, destination_table=destination_table)
            sql_statement += " ALTER TABLE {schema}.{source_table} RENAME TO {destination_table};\n".format(
                schema=self.schema, source_table=source_table, destination_table=destination_table)
            sql_statement += " GRANT SELECT ON {schema}.{destination_table} TO group dwh_readonly;\n".format(
                schema=self.schema, source_table=source_table, destination_table=destination_table)
            sql_statement += " GRANT UPDATE, INSERT, DELETE, REFERENCES, SELECT ON {schema}.{destination_table} TO group dwh_ddl_admin;\n".format(
                schema=self.schema, source_table=source_table, destination_table=destination_table)
        else:
            sql_statement += "DELETE FROM {schema}.{destination_table} USING {schema}.{source_table} WHERE \n".format(
                schema=self.schema, destination_table=destination_table, source_table=source_table)
            for i in range(0, len(unique_keys)):
                sql_statement += "{source_table}.{key} = {destination_table}.{key}".format(
                    source_table=source_table,
                    destination_table=destination_table,
                    key=unique_keys[i])
                if i < len(unique_keys) - 1:
                    sql_statement += " AND \n"

            sql_statement += "; \n"
            sql_statement += " INSERT INTO {schema}.{destination_table} SELECT * FROM {schema}.{source_table} ; \n".format(
                schema=self.schema, destination_table=destination_table, source_table=source_table)
        sql_statement += " COMMIT; \n"

        print(sql_statement)
        conn, cursor = self.__get_redshift_cursor()
        cursor.execute(sql_statement)
        cursor.close()
        conn.commit()
        print("Upsert command completed")

    def drop_table(self, table):
        """
        drop a given table
        Args:
            table: name of the table to be dropped
        Returns: None
        """
        # build the SQL statement
        drop_sql_statement = "DROP TABLE IF EXISTS {schema}.{table};".format(
            schema=self.schema, table=table)
        print(drop_sql_statement)
        conn, cursor = self.__get_redshift_cursor()
        cursor.execute(drop_sql_statement)
        cursor.close()
        conn.commit()
        print("Table dropped")

    def archive_to_s3(self, columns, table, conditions, s3_key, s3_bucket="hipages-long-lake",
                      s3_conn_id="s3_gandalf_write"):
        """
        Utility method used to archive a certain portion of a redshift table as csv compressed files onto s3.
        Args:
            s3_conn_id:
            s3_bucket:
            columns: List of columns in the sequence they are to be unloaded
            table: Name of the table to be unloaded
            conditions: A list of conditions that will all be ANDed in the WHERE clause
            s3_key: fully qualified s3 location
        Returns: None
        """
        query = """
                SELECT {columns}
                FROM {schema}.{table}
                WHERE {condition}
                """.format(
            columns=", ".join(columns),
            schema=self.schema,
            table=table,
            condition=" AND ".join(["1=1"] + conditions))
        self.unload_query_to_s3(query, s3_key, s3_bucket, s3_conn_id)

    def unload_query_to_s3(self,
                           query,
                           s3key,
                           s3bucket,
                           s3_conn_id,
                           delimiter='|',
                           s3_file_prefix='data_',
                           is_delimiter=True,
                           is_overwrite_allowed=True,
                           is_escape_allowed=True,
                           is_quotes_allowed=True,
                           is_header_allowed=False,
                           is_parallel_on=True,
                           compression="GZIP",
                           file_format='CSV'):
        """
        Internal method to read all files from the s3 source bucket and key, and load the files onto the given table
        Args:
            file_format: This can be either CSV or PARQUET
            compression:
            is_parallel_on: By default, UNLOAD writes data in parallel to multiple files,
        according to the number of slices in the cluster. If this value is False, data will be written to a single file.
            is_header_allowed:
            is_quotes_allowed:
            is_escape_allowed:
            is_overwrite_allowed:
            is_delimiter:
            query: SELECT query to unload
            s3key:
            s3bucket:
            s3_conn_id:
            delimiter:
            s3_file_prefix:
        Returns: None
        """
        # Escape single quotes in query
        escaped_query = query.replace("'", "''")

        # Retrieve aws credentials from conn string
        aws_access_key_id, aws_secret_access_key = self.__retrieve_aws_creds(s3_conn_id)

        header = ''
        delimiter_statement = "DELIMITER '{delimiter}'".format(delimiter=delimiter)
        overwrite_statement = 'ALLOWOVERWRITE'
        escape_statement = 'ESCAPE'
        quotes_statement = 'ADDQUOTES'
        parallel_statement = ''

        if compression is None:
            compression = ''

        if not is_delimiter:
            delimiter_statement = ''
        if not is_overwrite_allowed:
            overwrite_statement = ''
        if not is_escape_allowed:
            escape_statement = ''
        if not is_quotes_allowed:
            quotes_statement = ''
        if is_header_allowed:
            header = 'HEADER'
        if not is_parallel_on:
            parallel_statement = 'PARALLEL OFF'
        if file_format == 'PARQUET':
            delimiter_statement = ''
            quotes_statement = ''
            escape_statement = ''
            header = ''
            compression = ''
        if file_format == 'CSV':
            file_format = ''

        sql_statement = """
                UNLOAD ('
                {query}
                ')
                TO 's3://{s3bucket}/{s3key}/{s3_file_prefix}' 
                {header}
                {file_format}
                {delimiter_statement} 
                {overwrite} 
                {escape}
                {quotes}
                {parallel}
                {compression}
                """.format(
            query=escaped_query,
            s3bucket=s3bucket,
            s3key=s3key,
            s3_file_prefix=s3_file_prefix,
            header=header,
            file_format=file_format,
            delimiter_statement=delimiter_statement,
            overwrite=overwrite_statement,
            escape=escape_statement,
            quotes=quotes_statement,
            parallel=parallel_statement,
            compression=compression
        )
        print(sql_statement)
        sql_statement = sql_statement + """
                CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}' ;
                """.format(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key)
        conn, cursor = self.__get_redshift_cursor()
        cursor.execute(sql_statement)
        cursor.close()
        conn.commit()
        print("S3 data unloaded")

    def __retrieve_aws_creds(self):
        """
        Internal method to Retrieve aws credentials from conn string
        Returns: aws_access_key_id and aws_secret_access_key
        """
        session = boto3.Session()
        credentials = session.get_credentials()
        credentials = credentials.get_frozen_credentials()
        aws_access_key_id = credentials.access_key
        aws_secret_access_key = credentials.secret_key
        return aws_access_key_id, aws_secret_access_key

    def __get_redshift_cursor(self):
        """
        Internal method to Get a redshift cursor
        Returns:
        """
        conn = psycopg2.connect(host=self.host,
                                port=self.port,
                                dbname=self.schema,
                                user=self.user,
                                password=self.password)
        cursor = conn.cursor()
        return conn, cursor

    def get_table_schema(self, table):
        """
        Return a list of column,type,encoding,distkey,sortkey,notnull from redshift for the given table
        Args:
            table:
        Returns:
        """
        return self.execute_query("""
            SET search_path={schema};
            SELECT 
            "column",
            "type",
            "encoding",
            "distkey",
            "sortkey",
            "notnull" 
            FROM PG_TABLE_DEF 
            WHERE tablename = '{table}' 
            AND schemaname = '{schema}'
            """.format(
            schema=self.schema,
            table=table
        ))

    def get_athena_schema(self, table, file_format='CSV'):
        """
        Return a list of columns and datatypes based on the table schema from redshift
        Args:
            file_format: This can be either 'CSV' or 'PARQUET'
            table:
        Returns:
        """
        redshift_schema = self.get_table_schema(table)
        athena_columns = []
        for red_col in redshift_schema:
            athena_columns.append({
                'Field': red_col[0],
                'Type': self.convert_dtype_athena(red_col[1], file_format)
            })
        return athena_columns

    def convert_dtype_athena(self, redshift_type, file_format):
        """
        Converts the mysql data type statement to an athena data type
        Args:
            file_format: This can be either 'CSV' or 'PARQUET'
            redshift_type: string value containing mysql data type
        Returns: if the file_format is 'csv' this returns one of the four athena data types
                - INT, BIGINT, TIMESTAMP, STRING
            If the file_format is 'parquet' this returns one of the seven data types
                - BIGINT, DOUBLE, DECIMAL, BOOLEAN, STRING, TIMESTAMP, DATE
        """
        if file_format == 'CSV':
            return DTYPE_REDSHIFT_TO_CSV_ATHENA.get(str(self.simplified_dtype(redshift_type)), "STRING")
        elif file_format == 'PARQUET':
            parquet_data_type = DTYPE_REDSHIFT_TO_PARQUET_ATHENA.get(str(self.simplified_dtype(redshift_type)),
                                                                     "STRING")
            if parquet_data_type == 'DECIMAL':
                appends = re.search(r'\((.*?)\)', redshift_type).group(0)
                return parquet_data_type + appends
            else:
                return parquet_data_type
        else:
            print("Cannot find the provided file type")
            return

    def simplified_dtype(self, data_type):
        """
        Return the mysql base data type
        Args:
            data_type:
        Returns:
        """
        return ((re.sub(r'\(.*\)', '', data_type)).split(" ", 1)[0]).upper()
