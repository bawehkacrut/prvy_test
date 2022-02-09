import mysql
import mysql.connector
import pandas as pd
import logging
from dateutil import parser
from datetime import datetime, date
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition
from google.cloud.bigquery.table import TimePartitioning
from google.cloud import storage



def get_db_connection():
        """Get credentials from Airflow Connection"""

        host = '172.29.0.2'
        port = 3306
        user = 'user'
        password = 'password'
        db_name = 'db'

        try :
            connection = mysql.connector.connect(
                host=host,
                port=port,
                database=db_name,
                user=user,
                password=password
                )
            
        except Exception as err:
            raise Exception(err)

        return connection

def dump_csv_to_gcs(df, run_date, filename):

    # authorize key
    client = storage.Client()
    bucket = client.get_bucket('test')

    filename = f'production/documents/{run_date}/{filename}_{run_date}.csv'

    logging.info("Process: Will upload file: {} to GCS bucket".format(filename))   
    bucket.blob(filename).upload_from_string(df.to_csv(index=False, sep=';'), 'text/csv')
    logging.info("Process: Finish upload file: {} to GCS bucket".format(filename))


def extract_source_table(
    db_connection,
):

    query = """
    select *
    from documents
    """


    logging.info(
        "Process: Will run this query: \n"
        f"{query}"
    )

    cursor = db_connection.cursor(
        dictionary=True
    )
    # Execute query
    cursor.execute(query)
    # Fetch result query
    result = cursor.fetchall()

    # Close connection from DB
    cursor.close()
    db_connection.close()
    
    df = pd.DataFrame(result)

    return df


def DumpDBtoGCS(run_date, filename):

    connection = get_db_connection()

    logging.info(
        "Process: Will extract data from DB"
    )
    df = extract_source_table(connection)

    print(df)

    logging.info(
        "Process: Success extract data from DB"
    )

    dump_csv_to_gcs(df, run_date, filename)
