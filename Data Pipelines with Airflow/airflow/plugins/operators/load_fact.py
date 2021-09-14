from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table="",
                 sql="",
                 append=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append = append
        

    def execute(self, context):
        """
        Loads data from Redshift staging tables (staging_events & staging_songs) into the songplays table """
        self.log.info('Connect to Redshift')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append:
            redshift_hook.run("DELETE FROM {}".format(self.table))
            
        self.log.info("Load table from S3 into Redshift")
        redshift_hook.run("INSERT INTO {} {}".format(self.table,self.sql))
        
        self.log.info("Inserting into the fact table is successful")
