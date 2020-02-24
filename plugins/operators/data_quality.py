from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Performs data quality checks on fact and dim tables.
    For now, just makes sure they have some rows.  Redshift should enforce
    not null constraints, so we shouldn't have to worry about that.
    More thorough checks may be that format is correct,
    values lie within expected bounds, and there aren't duplicates.

    redshift_conn_id is the connection id for redshift
    tables is a list of tables to check quality
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def check_if_rows(self, rs_hook, table):
        """
        Checks if a table has at least 1 row.

        rs_hook is the redshift hook
        table is the name of a table as a string
        """
        records = rs_hook.get_records(f"SELECT COUNT(*) FROM {table}")

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")

        num_records = records[0][0]

        if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")

        self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")


    def execute(self, context):
        self.log.info('Performing data quality checks.')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for t in self.tables:
            self.check_if_rows(redshift_hook, t)
