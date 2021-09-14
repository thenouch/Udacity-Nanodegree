from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY'{}'
    TIMEFORMAT as 'epochmillisecs'
    JSON '{}'
    """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json

    def execute(self, context):
        aws_conn = AwsHook(self.aws_credentials)
        
        aws_credentials = aws_conn.get_credentials()
        
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info(f'Starting transfer for staging data from {self.s3_bucket} | {self.s3_key} to {self.table}')
        self.log.info(f'Deleting data from {self.table}')
        
        redshift_hook.run(f"DELETE FROM {self.table}")
        self.log.info(f'Copying data from {self.s3_bucket} | {self.s3_key} to {self.table}')
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
      
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            self.json)
        
        redshift_hook.run(formatted_sql)
        self.log.info(f'Copying of staging data completed.')   





