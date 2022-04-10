import pandas as pd
import numpy as np
import json
from sodapy import Socrata
import boto3
from clean_data import clean
import os

def json_for_copy(dataframe, file_name):
    """Define a custom version of offloading pandas dataframes to SQL-Compliant JSONs.
    In pandas, to_json() will produce JSON files like 
    I) [[<values></values>],[<values></values>],etc.] 
    or 
    II) {{"column1":"row1value1","column2":"rowvalue2",...,"columnN":"rowvalueN"}}. 
    This function strips away the outer layers and returns 
    I) [<values></values>]
       [<values></values>]
    
    II) {"column1":"row1value1","column2":"row1value2",...,"columnN":"row1valueN"}
        {"column1":"row2value1","column2":"row2value2",...,"columnN":"row2valueN"}
    """
    write_data = dataframe.to_json(orient = 'records')[1:-1].replace('},{','}\n{')

    f = open(file_name,'w')

    f.writelines(write_data)

    f.close()

    return 1

# Get credentials in keys file stored in parent directory
cwd = os.getcwd()
keys_file = os.path.join(cwd, 'keys.json')
keys = json.load(open(keys_file))

aws_id = keys['AWS']['KEY']
aws_secret = keys['AWS']['SECRET']

session = boto3.Session(
    aws_access_key_id=aws_id,
    aws_secret_access_key=aws_secret,
)
bucket = keys['S3']['Bucket']
s3 = session.resource('s3')


# Data From https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i        
data_url='data.cityofnewyork.us'    # The Host Name for the API endpoint (the https:// part will be added automatically)
data_set='qgea-i56i'    # The data set at the API endpoint 
app_token= keys['NYC_Data']['App']['token']   # The app token created in prior steps
client = Socrata(data_url,app_token)      # Create the client to point to the API endpoint
# Set the timeout to 60 seconds    
client.timeout = 60




for yr in [2017,2018,2019]:
    query = f"date_extract_y(rpt_dt) = {yr}"

    # Get data in JSON format fromNYC Open Data  API
    crime_data = client.get(data_set, where=query, limit = 8*10**6)
    
    #Convert JSON to dataframe
    crime_df = pd.DataFrame.from_records(crime_data)

    # Apply custom cleaning function
    df_clean = clean(crime_df)

    # Write file for each year
    file_name = f"nyc_crime_data_{yr}.json"
    print(yr,"Cleaning Done.")

    # Save dataframe as json using custom json format that redshift expects.
    json_for_copy(dataframe = df_clean,file_name=file_name)

    # Upload cleaned data to S3 in JSON Format
    s3_rel_path = 'Crime-Data-NYC/' + file_name
    s3.meta.client.upload_file(Filename=file_name, Bucket=bucket, Key=s3_rel_path)
    print(yr,"Upload Done.")


exit()
