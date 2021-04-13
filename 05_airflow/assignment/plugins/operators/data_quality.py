from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 dq_checks = {},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.dq_checks = dq_checks
        
    def execute(self, context):
        self.log.info('DataQualityOperator')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        
        error_count = 0
        failed_cases = []
        for dq_stmt, expect_result in self.dq_checks.items():
            result = redshift.get_records(dq_stmt)[0][0]
            if result != expect_result:
                error_count += 1
                failed_cases.append(dq_stmt)
        
        if error_count:
            self.log.warning(f"number of failed test case : {error_count}")
            self.log.warning(failed_cases)
            raise ValueError("Data quality check failed")
            
        self.log.warning("Data quality check passed")
                
        
        
        