import requests
import json
import time
import csv
import os
import pandas as pd
from mailchimp3 import MailChimp
from datetime import datetime
from variables import MUSER


headers = requests.utils.default_headers()


## the function will get all audience members, clean the data in a dataframe, and make it a csv for ETL
def get_audience_csv(api_key, aid):
    ## measuring algo runtime
    start = time.time()

    client = MailChimp(mc_api=api_key, mc_user=MUSER)

    ## change to get all members, returns a list of dicts, each dict an audience member 
    # subs = client.lists.members.all(aid, count = 100) 
    subs = client.lists.members.all(aid, get_all=True)
    
    ## create pandas df to structure the data appropriately
    df = pd.DataFrame(subs['members'])

    ## get separate df to make merge_fields standalone columns
    mf_dict = df['merge_fields'].to_dict()

    ## putting in dataframe, transposing to get column names
    mf_df = pd.DataFrame(mf_dict).T

    ## variable for concatenation
    frames = [df, mf_df]

    ## outer joining the two dataframes
    db_df = pd.concat(frames, axis=1, join="outer")

    ## dropping original merge_field col and _links
    db_df.drop("merge_fields", axis=1, inplace=True)
    db_df.drop("_links", axis=1, inplace=True)


    ## standardizing column casing
    standard_columns = []
    for j in db_df.columns:
        standard_columns.append(j.lower())

    db_df.columns = standard_columns

    ## cleaning the stats column, each value goes to its own column
    or_list = []
    cr_list = []

    for x_dict in db_df['stats']:
        or_list.append(float(x_dict['avg_open_rate']))
        cr_list.append(float(x_dict['avg_click_rate']))

    db_df['avg_open_rate'] = or_list
    db_df['avg_click_rate'] = cr_list

    ## cleaning location columns, getting coordinates and country code on sep columns
    lat_long = []
    count_code = []

    for i in db_df.location:
        lat_long.append((i['latitude'], i['longitude']))
        count_code.append(i['country_code'])

    db_df['coordinates'] = lat_long
    db_df['country_code'] = count_code

    ## dropping cleaned columns
    db_df.drop("location", axis=1, inplace=True)
    db_df.drop("stats", axis=1, inplace=True)
    db_df.drop("unsubscribe_reason", axis=1, inplace=True)

    ## cleaning up the 'tags' field to only have tag names in a new column
    ## transpose to iterate over tags at axis=1
    dft = df.T

    ## list to be populated for each user's tags
    mtag_list = []

    for i in range(len(db_df)):
        utags = []
        s = str(dft[i]['tags'])
        ## making into json format for dict manipulation
        jss = s.replace("'", "\"")

        d = json.loads(jss)
        ## if no tags, add blank space 
        if d == list():
            utags.append(" ")

        elif len(d) > 0:
            for i in d:
                utags.append(i['name'])
            
        mtag_list.append(utags)


    db_df['tags_names'] = mtag_list

    db_df.drop("tags", axis=1, inplace=True)


    print("----------------------------------------")
    print("DATA CLEANING ALGO\n")
    print("Mailchimp audience member export is ready! Check for CSV file on your current directory: {}\n".format(os.getcwd()))
    
    end = time.time()

    algo_runtime = (end-start)

    ## generating timestamp for distinction
    now = datetime.now()
    dt_nformat = now.strftime("%d/%m/%Y %H:%M:%S")

    print("CSV created on: {}\n".format(dt_nformat))
    print("Algorithm runtime: {} seconds \n".format(algo_runtime))
    print("----------------------------------------")


    csv_db = db_df.to_csv("mc_audience_export.csv", sep="|", index=False, header=False)

    return csv_db

## function CALL
# get_audience_members_csv(API_KEY_PROD, AID_PROD)