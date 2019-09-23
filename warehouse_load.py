
import snowflake.connector
import pandas as pd
import time
import sys
import os
from variables import USER, PASSWORD, ACCOUNT, WAREHOUSE, DB_COLUMNS

def db_etl():

    start = time.time()

    ## hardcoded (schema established), from variables file
    df_cols = DB_COLUMNS

    try:
        ctx = snowflake.connector.connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            warehouse=WAREHOUSE
            )

        cs = ctx.cursor()

    except Exception as error:
        print("Error message: {}\n".format(error))


    print("----------------------------------------")
    print("ETL ALGO\n")
    # create db if it doesn't exist for our mailchimp audience; use that for future execution
    print("Creating database and schema for use...\n")

    try: 
        cs.execute("CREATE DATABASE IF NOT EXISTS mailchimp_audience") 
        cs.execute("USE DATABASE mailchimp_audience")
        cs.execute("USE SCHEMA mailchimp_audience.public")

        print("Creating contacts table...\n")

        table_create = "CREATE TABLE IF NOT EXISTS contacts ({} varchar PRIMARY KEY, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar, {} varchar)".format(df_cols[0], df_cols[1], df_cols[2], df_cols[3], df_cols[4], df_cols[5], df_cols[6], df_cols[7], df_cols[8], df_cols[9], df_cols[10], df_cols[11], df_cols[12], df_cols[13], df_cols[14], df_cols[15], df_cols[16], df_cols[17], df_cols[18], df_cols[19], df_cols[20], df_cols[21], df_cols[22], df_cols[23], df_cols[24], df_cols[25], df_cols[26], df_cols[27], df_cols[28], df_cols[29], df_cols[30], df_cols[31], df_cols[32], df_cols[33])
        cs.execute(table_create)

        ## data loading format
        print("Creating file format and stage for files...\n")
        cs.execute(""" CREATE OR REPLACE FILE FORMAT csv_form type='CSV' record_delimiter = '\\n' field_delimiter = '|' field_optionally_enclosed_by = '''' """)

        ## create internal stage area for loading
        cs.execute("CREATE OR REPLACE STAGE csv_stage FILE_FORMAT = csv_form")

        print("Putting file into our stage...\n")
        cs.execute("PUT file://{}/mc_audience_export.csv @csv_stage auto_compress = true".format(os.getcwd()))


    except Exception as error:
        print('Error message: {}\n'.format(error))
        print('Terminating process...\n')
        sys.exit(1)
        

    ## SQL query to copy our CSV into target table
    q = "COPY INTO contacts FROM @csv_stage/mc_audience_export.csv.gz FILE_FORMAT = (format_name = csv_form) ON_ERROR = 'abort_statement'"

    try:
        print("Copying file (below) from stage into target table...")
        cs.execute(q)
        one_row = cs.fetchone()
        
        print("**", one_row[0], '\n')

    finally:
        print("Removing file from stage...\n")
        cs.execute("REMOVE @csv_stage pattern = '.*.csv.gz'")

        print("Closing connection...\n")
        cs.close()

        print("Closed...\n")


    ctx.close()

    end = time.time()
    algo_runtime = (end-start)

    print("ETL algorithm runtime: {} seconds\n".format(algo_runtime))
    print("----------------------------------------")
