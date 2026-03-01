import pandas as pd
import pantab
import dotenv
import os
import snowflake.connector
import tableauserverclient as TSC
from execution.logger import logger_setup

logger = logger_setup()

dotenv.load_dotenv()

snowflake_user = os.getenv("snowflake_user")
snowflake_password = os.getenv("snowflake_password")
snowflake_account = os.getenv("snowflake_account")
snowflake_schema = os.getenv("snowflake_schema")
snowflake_database = os.getenv("snowflake_database")

tableau_user = os.getenv("tableau_user")
tableau_password = os.getenv("tableau_password")
tableau_server = os.getenv("tableau_server")
tableau_site = os.getenv("tableau_site")

def create_hyper(snowflake_table="MARTS_ACEA_METRICS", hyper_path="hyper_files/marts_acea.hyper"):

    """
    Creating hyper file for Tableau datasource upload.
    :param snowflake_table: name of the table in Snowflake to read data from
    :param hyper_path: path to save the generated hyper file
    """
    
    try:
        conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            schema=snowflake_schema,
            database=snowflake_database
        )
        
        mart_acea_df = pd.read_sql(f'SELECT * FROM {snowflake_table}', conn)
        logger.info(f"Data read from Snowflake: {mart_acea_df.shape[0]} rows, {mart_acea_df.shape[1]} columns")
        
        pantab.frame_to_hyper(mart_acea_df, hyper_path, table="marts_acea_metrics")
        logger.info(f"Hyper file created at: {hyper_path}")

        conn.close()

    except Exception as e:
        logger.error(f"Error creating hyper file: {e}")
        raise

def upload_to_tableau(project_name):
    """
    Upload hyper file to Tableau Server.
    :param project_name: name of the Tableau project to publish the datasource to
    """
    try:
        tableau_auth = TSC.TableauAuth(tableau_user, tableau_password, site_id=tableau_site)
        server = TSC.Server(tableau_server, use_server_version=True)
        
        with server.auth.sign_in(tableau_auth):

            # need to specify the project where datasource will be published
            req_option = TSC.RequestOptions()
            req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,     
                                 TSC.RequestOptions.Operator.Equals,
                                 project_name))
            project_item, pagination = server.projects.get(req_option)
            logger.info(f"Project retrieved: {project_name}")
            for p in project_item:
                project_id = p.id

            new_datasource = TSC.DatasourceItem(project_id)
            server.datasources.publish(
                new_datasource,
                file="hyper_files/marts_acea.hyper",
                mode='Overwrite'
            )
            logger.info(f"Datasource published to project: {project_name}")
    except Exception as e:
        logger.error(f"Error uploading to Tableau: {e}")
        raise

if __name__ == "__main__":
    try:
        create_hyper()
        upload_to_tableau('Charles')
    except Exception as e:
        exit(1)



