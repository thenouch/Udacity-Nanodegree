from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append = append
        

    def execute(self, context):
        """
        Loads data from Redshift staging tables (staging_events & staging_songs) into the dimensional tables """
        self.log.info(f'Loading data transfer to {self.table}')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append:
            
            self.log.info(f"Removing data from {self.table}")
            redshift_hook.run(f"DELETE FROM {self.table}")
            
            
        self.log.info(f"Loading dimension table:  {self.table} ")
        
        redshift_hook.run("INSERT INTO {} {}".format(self.table,self.sql))
        
        self.log.info("Inserting into the dimensional table is successful")
        
                      
