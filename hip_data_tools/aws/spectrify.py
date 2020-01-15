from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.redshift import RedshiftUtil
from hip_data_tools.aws.s3 import S3Util


class RedshiftSpectrifyUtil:
    """
    Utility to convert a table in redshift to a table in athena by transferring data in csv/parquet format
    """

    def __init__(self,
                 table_name,
                 redshift_schema,
                 s3_bucket,
                 athena_database,
                 redshift_conn_id,
                 s3_conn_id,
                 athena_conn_id,
                 s3_dir,
                 file_format='CSV'):
        """
        Init function
        Args:
            table_name (str):
            redshift_schema (str):
            s3_bucket (str):
            athena_database:
            redshift_conn_id:
            s3_conn_id:
            athena_conn_id:
            file_format: This value should be either 'CSV' or 'PARQUET'
        """
        self.ru = RedshiftUtil(schema=redshift_schema, redshift_conn_id=redshift_conn_id)
        self.s3u = S3Util(conn=s3_conn_id, bucket=s3_bucket)
        self.au = AthenaUtil(conn=athena_conn_id, database=athena_database)
        self.table_name = table_name
        self.s3_dir = s3_dir
        self.file_format = file_format

    def clean_s3_dir(self):
        self.s3u.delete_recursive(key_prefix = self.s3_dir)

    def prepare_unload_query(self, athena_schema):
        return """select {columns} from {schema}.{table}""".format(
            columns=", ".join([dct["Field"] for dct in athena_schema]),
            schema=self.ru.schema,
            table=self.table_name)

    def prepare_athena_table_ddl(self, athena_schema):
        row_format = '\'org.apache.hadoop.hive.serde2.OpenCSVSerde\''
        if self.file_format == 'PARQUET':
            row_format = '\'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\''
        return """
CREATE EXTERNAL TABLE IF NOT EXISTS `{table}` (
  {column_list}
)
ROW FORMAT SERDE {row_format}
WITH SERDEPROPERTIES (
    'separatorChar' = '|',
    'quoteChar' = '\\"',
    'escapeChar' = '\\\\'
) LOCATION 's3://{s3_bucket}/{s3_key}/'
TBLPROPERTIES ('has_encrypted_data'='false');
                """.format(
            table=self.table_name,
            column_list=", ".join(["{} {}".format(dct["Field"], dct["Type"]) for dct in athena_schema]),
            row_format=row_format,
            s3_bucket=self.s3u.bucket,
            s3_key=self.s3_dir)

    def spectrify_complete_table(self):
        athena_schema = self.ru.get_athena_schema(self.table_name, self.file_format)
        unload_query = self.prepare_unload_query(athena_schema)
        self.clean_s3_dir()
        self.ru.unload_query_to_s3(
            query=unload_query,
            s3key=self.s3_dir,
            s3bucket=self.s3u.bucket,
            s3_conn_id=self.s3u.conn,
            file_format=self.file_format
        )
        athena_ddl = self.prepare_athena_table_ddl(athena_schema)
        self.au.run_query(query_string=athena_ddl)
