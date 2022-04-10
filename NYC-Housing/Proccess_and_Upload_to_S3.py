import os
import boto3
import pandas as pd
import sys
import json
from io import StringIO 
import re
from custom_cleaning_funs import * #Read in custom cleaning functions designed in EDA.

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
        {"column1":"row1value1","column2":"row1value2",...,"columnN":"row1valueN"}
    """
    write_data = dataframe.to_json(orient = 'records')[1:-1].replace('},{','}\n{')

    f = open(file_name,'w')

    f.writelines(write_data)

    f.close()

    return 1

cwd = os.getcwd()
keys_file = os.path.join(cwd, 'keys.json')
keys = json.load(open(keys_file))

# Create S3 session that we will use to upload proccesses json to s3.
# If the dataset was larger we would rather copy json directly to  Refshift then do the preproccessing in staging schema using SQL.
aws_id = keys['AWS']['KEY']
aws_secret = keys['AWS']['SECRET']

session = boto3.Session(
    aws_access_key_id=aws_id,
    aws_secret_access_key=aws_secret,
)
bucket = keys['S3']['Bucket']
# for uploading to S3
s3 = session.resource('s3')

#For downloading from S3
s3_client = boto3.client('s3')


# Got from AWS Data Marketplace (https://aws.amazon.com/marketplace/pp/prodview-27ompcouk2o6i?sr=0-6&ref_=beagle&applicationId=AWSMPContessa#overview)
nyc_housing_data_s3_keys = ['50782dc315b94e46fdbd4a12cec6820e/96023397ee826914fefcef392b218c7b/2017_NYC_Property_Sales__10172019 .csv', 
'7d8f73e3c5acdde79fd2874dd98afdcd/2665bb59124746c73ba2c36b60b29d60/2016_NYC_Property_Sales__10212019.csv',
'fc19d00c8780199e4fccd21f4834c905/b0457c8b3c201115daa0f6ca8f2c4140/2018_NYC_Property_Sales__10172019.csv']

housing_datasets = []
for object_key_value in nyc_housing_data_s3_keys:
    dataset = get_and_rename(s3_client, bucket, object_key_value)
    cleaned_dataset = clean(dataset)
    housing_datasets.append(cleaned_dataset)
    


# Join each dataset into one big table.
nyc_housing_data = pd.concat(housing_datasets)


# Extract building category information
extracted_building_fields = []

for i in nyc_housing_data['building_class_category']:
    extracted_building_fields.append(extractBuildingClassFields(i))

nyc_housing_data['building_class_code'] = [i[0] for i in extracted_building_fields]
nyc_housing_data['building_class_type'] = [i[1] for i in extracted_building_fields]
nyc_housing_data['building_class_details'] = [i[2] for i in extracted_building_fields]
nyc_housing_data = nyc_housing_data.drop('building_class_category',axis = 1)


#  Format Sale Dates
nyc_housing_data['sale_date'] = nyc_housing_data['sale_date'].apply(format_date)

# Reset index or else to_json will throw an error below.
nyc_housing_data_final = nyc_housing_data.reset_index(drop = True)

# Write data to local JSON, then upload JSON to S3 to that it can be copied from S3 to Redshift
# Similary, the S3 data could be used in an EMR Cluster for big data operations or machine learning. 
file_name = 'enigma_property_sales.json'
s3_rel_path = 'Housing-Data/' + file_name
json_for_copy(dataframe=nyc_housing_data_final,file_name=file_name)

s3.meta.client.upload_file(Filename=file_name, Bucket=bucket, Key=s3_rel_path)