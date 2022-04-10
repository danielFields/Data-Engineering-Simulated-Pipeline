import os
import boto3
import pandas as pd
import sys
from io import StringIO 
import re


def clean_numbers(x):
    """
    This function scrubs number fields of non-numeric characters and converts them to a float.
    In the instance where is not any values, we return 0.
    """
    x_scrubbed = re.sub(r"[^0-9.]", "", x)   
    if x_scrubbed == '':
        return 0
    else:
        return float(x_scrubbed)
        

def get_and_rename(client, bucket_name, object_key):
    """
    This function retrives data from S3 and renames the columns 
    becuase the retrieved data columns have an extra '\n' at each column.
    """
    # Create a csv buffer to allow pandas to read in json data
    csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')

    # Change all columns to be lowercase for easier typing out of names
    df = pd.read_csv(StringIO(csv_string))
    df = df.rename(columns = lambda x: x.replace("\n","")\
        .replace(" ","_")\
        .lower())


    return df

def clean(df):
    """
    This function cleans the raw data from AWS Marketplace to a workable format.
    This function is custom made for this dataset and was develloped in a python notebook.
    This function will rename columns, assign column types as desired for analysis, and 
    return a cleaned dataframe for additional processing.
    """
    # Manually assign column types
    col_types = {'commercial_units':'int64',
        'gross_square_feet':'float64',
        'residential_units':'float64',
        'sale_price':'float64',
        'borough':'int64',
        'total_units':'int64',
        'block':'int64',
        'zip_code':'object',
        'sale_date':'object',
        'neighborhood':'object',
        'land_square_feet':'float64',
        'building_class_category':'object',
        'ease-ment':'object',
        'lot':'object',
        'borough_name':'object',
        'year_built':'int64',
        'tax_class_at_time_of_sale':'object',
        'apartment_number':'object',
        'address':'object',
        'building_class_at_time_of_sale':'object'}

    # Change these columns from All Caps to title case
    caps_columns = ['neighborhood','building_class_category','address','borough_name']

    df[caps_columns] = df[caps_columns]\
    .astype(str)\
    .applymap(lambda x: x.title())
        

    # Clean floating point number columns (remove all $,- etc)
    num_columns = ['land_square_feet','sale_price','residential_units','gross_square_feet','commercial_units','total_units']
    df[num_columns] = df[num_columns].astype(str).applymap(clean_numbers)

    # Return dataframe with specified data types
    #Ignore incorrectky formatted number since during EDA all found examples were not useful data.
    df = df.astype(col_types, errors = 'ignore') 

    return df[list(col_types.keys())]
    
    
def extractBuildingClassFields(x):
    """
    Building Catgories are given in the returned json format as '08 Rentals - Elevator Apartments'
    This fucntion splits the building category into Building Code (08), Building Type (Rentals), and Building Details (Elevator Apartments)
    """
    code = ''
    building_type = ''
    building_details = ''

    first_space = x.index(' ')

    code = x[0:first_space]

    rem = x[first_space::].strip()

    hasSpecifications = rem.find('-') > -1 or rem.find('/')
    specs = [rem,'']
    if hasSpecifications:
        if rem.find('-') > -1:
            specs = rem.split('-',1)
        elif rem.find('/') > -1:
            specs = rem.split(' ',1)

        

    extracted_info = [code] +  specs

    extracted_clean = [i.strip() for i in extracted_info]
    
    return extracted_clean

def format_date(raw_str):
    """
    Convert Date String from mm/dd/yy -> YYYY-MM-DD
    """
    month,day,year = raw_str.split("/")

    month = int(month)
    day = int(day)

    if month < 10:
        month = '0' + str(month)
    else:
        month = str(month)

    if day < 10:
        day = '0' + str(day)
    else:
        day = str(day)
    if len(year) == 2:
        year = '20' + str(year)

    return f"{year}-{month}-{day}"