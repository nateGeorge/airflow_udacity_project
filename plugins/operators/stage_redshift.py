from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Copies JSON files from an S3 bucket into a Redshift staging table.
    The table is dropped first if it exists.

    The log files at udacity-dend/log_data/ are formatted as
    s3://udacity-dend/log_data/{year}/{day}/{date}-events.json
    so we have a json file for events each day, and want to only load
    json files for the day of execution when running.
    """
#     ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_config="auto",
                 # execution_date="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_config = json_config
        self.copy_sql = """COPY {} FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                json '{}';
                """
        # self.execution_date = execution_date

    def execute(self, context):
        # self.log.info(f'execution date {self.execution_date}')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing Redshift staging table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        # the s3 path should be checked if it exists -- if not, a ValueError should be thrown
        # https://stackoverflow.com/q/33842944/4549682

        self.log.info(f"Copying JSON data from S3 path {s3_path} to Redshift table {self.table}")

        formatted_sql = self.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_config
        )

        redshift.run(formatted_sql)
