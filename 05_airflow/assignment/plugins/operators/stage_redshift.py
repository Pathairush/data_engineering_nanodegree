from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_field = ("s3_key",)
    copy_stmt = '''
                COPY {}
                FROM '{}'
                IAM_ROLE '{}'
                REGION '{}'
                FORMAT AS JSON '{}'
                '''
     
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 iam_role = "",
                 region = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 json_path = "auto",
                 create_stmt = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.iam_role = iam_role
        self.region = region
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.create_stmt = create_stmt

    def execute(self, context):
        
        self.log.info('StageS3ToRedshift Operator')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        
        self.log.info(f'Clearing data on redshift : {self.table}')
        redshift.run(f"DROP TABLE IF EXISTS {self.table}")
        
        self.log.info(f'Creating staging table on redshift : {self.table}')
        redshift.run(self.create_stmt)
        
        self.log.info(f'Copying data from S3 to Redshift : {self.table}')
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        formatted_sql = StageToRedshiftOperator.copy_stmt.format(
            self.table,
            s3_path,
            self.iam_role,
            self.region,
            self.json_path
        )     
        redshift.run(formatted_sql)
        
        




