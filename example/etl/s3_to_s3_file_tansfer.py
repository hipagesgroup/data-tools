"""

Example Script to transfer S3 files from one bucket to the other

"""
from hip_data_tools.aws.common import AwsConnectionSettings, AwsSecretsManager
from hip_data_tools.etl.s3 import AddTargetS3KeyTransformer, S3SinkSettings, S3SourceSettings
from hip_data_tools.etl.s3_to_s3 import S3ToS3FileCopy


def transfer_files():
    # These Aws setting assume that you have your Aws Access keys in the Standard env vars
    aws_setting = AwsConnectionSettings(
        region="ap-southeast-2",
        secrets_manager=AwsSecretsManager(),
        profile=None)

    #  If you want to use the Aws profiles stored by the swd cli tool, uncomment the following code:
    # aws_setting = AwsConnectionSettings(
    #     region="us-east-1",
    #     secrets_manager=None,
    #     profile="default")

    # Define the ETL instance
    etl = S3ToS3FileCopy(
        source=S3SourceSettings(
            bucket="my_source_bucket",
            key_prefix="source/prefix",
            suffix=None,
            connection_settings=aws_setting,
        ),
        sink=S3SinkSettings(
            bucket="my_target_bucket",
            connection_settings=aws_setting,
        ),
        transformers=[AddTargetS3KeyTransformer(target_key_prefix="target/prefix")],
    )
    # Check the files that will be transferred
    files = etl.list_source_files()
    # If you want to transfer files in a loop
    while etl.has_next():
        etl.execute_next()

    # Reset the source state
    etl.reset_source()

    # If you want to transfer all files sequentially
    etl.execute_all()


if __name__ == "__main__":
    transfer_files()
