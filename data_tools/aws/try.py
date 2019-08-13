from common import log
from data_tools.aws.athena import AthenaUtil
from authenticate import AwsConnection

conn = AwsConnection(mode="assume_role", settings={
    "profile_name": "default"
})

client = conn.get_client(client_type='s3')

buckets = client.list_buckets()
log.info(buckets['Buckets'])

conn = AwsConnection(mode="standard_env_var", settings={})

client = conn.get_client(client_type='s3')

buckets = client.list_buckets()
log.info(buckets['Buckets'])

conn = AwsConnection(mode="custom_env_var", settings={
    "aws_access_key_id_env_var": "aws_access_key_id",
    "aws_secret_access_key_env_var": "aws_secret_access_key"
})

client = conn.get_client(client_type='s3')

buckets = client.list_buckets()
log.info(buckets['Buckets'])

au = AthenaUtil(database="long_lake", conn=conn, output_bucket="au-com-hipages-data-scratchpad",
                output_key="tmp/scratch/")
result = au.run_query("SELECT * FROM dim_date limit 10", return_result=True)
log.warning(result)
