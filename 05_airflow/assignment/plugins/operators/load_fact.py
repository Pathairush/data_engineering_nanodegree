from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 create_stmt = "",
                 insert_stmt = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.create_stmt = create_stmt
        self.insert_stmt = insert_stmt

    def execute(self, context):
        self.log.info('LoadFactOperator')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        
        self.log.info('Create fact table if not exist')
        redshift.run(self.create_stmt)
        
        self.log.info('Insert data to fact table')
        redshift.run(self.insert_stmt)
        
