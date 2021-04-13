from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table="",
                 is_delete_load=True,
                 create_stmt = "",
                 insert_stmt = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.is_delete_load = is_delete_load
        self.create_stmt = create_stmt
        self.insert_stmt = insert_stmt

    def execute(self, context):
        self.log.info('LoadDimensionOperator')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
                
        if self.is_delete_load:
            
            self.log.info(f'Trigger delete-load')
            self.log.info('Drop dimension table if exists')
            redshift.run(f"DROP TABLE IF EXISTS public.{self.table}")
        
            self.log.info('Crate dimension table')
            redshift.run(self.create_stmt)

            self.log.info('Insert data to dimension table')
            redshift.run(self.insert_stmt)
            
        else:
            
            self.log.info(f'Trigger append-only')
            self.log.info('Insert data to dimension table')
            redshift.run(self.insert_stmt)