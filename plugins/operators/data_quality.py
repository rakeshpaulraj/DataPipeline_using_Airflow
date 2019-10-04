from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 table=[],
                 test_cases=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

        
    def execute(self, context):
        self.log.info("Fetching the redshift hook..")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("***************************************************************************")
        self.log.info("Running Data Quality checks for table - {self.table} ...")
        self.log.info("***************************************************************************")
        
        # Check for zero rows
        for table in self.table:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality test - {table} table - check for zero rows - FAILED. Returned no results")

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality test - {table} table - check for zero rows - FAILED. Returned 0 rows")
            self.log.info(f"Data quality test - {table} table - check for zero rows - PASSED. {records[0][0]} records found")

        # Check custom test cases
        for i, t in enumerate(self.test_cases):
            test_name=t['name']
            sql=t['sql']
            exp_result=t['exp_result']
            self.log.info(f"Executing test case #{i}..")
                  
            records = redshift.get_records(sql)
            actual_result = records[0][0]
            self.log.info(f"Query output = {actual_result}")
            if actual_result != exp_result:
                self.log.error(f"Data quality test - {test_name} - FAILED. Expected result = {exp_result}, Actual result = {actual_result}")
                raise ValueError(f"Data quality test - {test_name} - FAILED. Expected result = {exp_result}, Actual result = {actual_result}")
            
            self.log.info(f"Data quality test - {test_name} - PASSED. Expected result = {exp_result}, Actual result = {actual_result}")
            
                  
                  