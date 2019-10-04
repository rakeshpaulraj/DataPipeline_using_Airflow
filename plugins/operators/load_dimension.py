from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        self.log.info("Fetching the redshift hook..")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from {self.table} Dimension table in Redshift..")
        redshift.run("TRUNCATE TABLE {}".format(self.table))  
        
        self.log.info(f"Loading data into {self.table} Dimension table in Redshift..")
        formatted_sql=f"INSERT INTO {self.table} {self.sql}"
        redshift.run(formatted_sql)
