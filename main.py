from variables import API_KEY_PROD, AID_PROD
from warehouse_load import db_etl 
from get_source_data import get_audience_csv


def main():
    get_audience_csv(API_KEY_PROD, AID_PROD)  ## Create CSV with full mailchimp audience
    
    db_etl()  ## Stage CSV of audience on Snowflake, upload data

    print("ETL process finished! \n")

if __name__ == "__main__":
    main()