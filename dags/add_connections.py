from airflow import settings
from airflow.models import Connection
aws_credentials = Connection(
        conn_id='aws_credentials',
        conn_type='Amazon Web Services',
        host='',
        login='',
        password=''
)
redshift = Connection(
        conn_id='redshift',
        conn_type='Postgres',
        host='udacity-dend-p5.cvxtjfgdtfwc.us-west-2.redshift.amazonaws.com',
        login='awsuser',
        password='',
        port='5439',
        schema='dev'
)
session = settings.Session()
session.add(aws_credentials)
session.add(redshift)
session.commit()
