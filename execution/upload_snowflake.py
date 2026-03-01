import pandas as pd
import datetime
import dotenv
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from execution.logger import logger_setup

dotenv.load_dotenv()
logger = logger_setup()

def upload_to_snowflake(): 

    snowflake_user = os.getenv("snowflake_user")
    snowflake_password = os.getenv("snowflake_password")
    snowflake_account = os.getenv("snowflake_account")
    snowflake_schema = os.getenv("snowflake_schema")
    snowflake_database = os.getenv("snowflake_database")

    # fetch ACEA csv files in data folder only
    data_dir = "data/"
    csv_paths = []
    for root, dires, files in os.walk(data_dir):
        csv_files = [file for file in files if file.startswith('Press_release') and file.endswith('.csv')]
        csv_paths.extend([os.path.join(root, file) for file in csv_files])
    
    logger.info(f"Found {len(csv_paths)} ACEA Press_release CSV files for upload")

    if not csv_paths:
        raise ValueError("No ACEA Press_release CSV files found in data/ for upload")

    df = pd.concat([pd.read_csv(path) for path in csv_paths], ignore_index=True)
    
    # clean data: remove commas from Units column and convert to numeric
    df['Units'] = df['Units'].astype(str).str.replace(',', '').astype(float)
    
    # add a timestamp column to the dataframe
    df['inserted_at'] = datetime.datetime.now().isoformat()

    # snowflake connection
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        schema=snowflake_schema,
        database=snowflake_database
        )

    # writing data to table
    write_pandas(conn, 
                 df, 
                 'ACEA_DATA', 
                 schema=snowflake_schema, 
                 database=snowflake_database,
                 quote_identifiers=False,
                 overwrite=True,
                 auto_create_table=True)

    # delete uploaded ACEA files after upload
    for path in csv_paths:
        os.remove(path)
    
    logger.info(f"Uploaded {len(df)} records to Snowflake and deleted local CSV files")    

    # close connection
    conn.close()

if __name__ == "__main__":
    upload_to_snowflake()