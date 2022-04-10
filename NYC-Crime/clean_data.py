import pandas as pd
import re

def crime_type(crime_desc):
    """
    This function is used to extract the crime type from the crime description that has several codes and misc info in it.
    This function will take in a crime description and then:
    1. Remove numbers
    2. Find Location of first comma
    3. Check if comma exists
    4. If comma exists, take substring from 0:first_comma_index
        Else take whole cleaned string
    Examples:
        crime_type('Robbery,Open Area Unclassified') -> 'Robbery'
        crime_type('Criminal Mischief,Unclassified 4') -> 'Criminal Mischief'
        crime_type('Assault 3') -> 'Assault'    
    """
    scrubbed = re.sub(r'[0-9]','',crime_desc).strip()

    first_comma = scrubbed.find(',')

    if first_comma > -1:
        scrubbed = scrubbed[0:first_comma]
    
    return scrubbed


def clean(nyc_crime_raw):

    use_columns = ['boro_nm',
                'cmplnt_num',
                'cmplnt_fr_dt',
                'cmplnt_to_dt',
                'hadevelopt', 
                'law_cat_cd',
                'loc_of_occur_desc', 
                'pd_desc', 
                'prem_typ_desc', 
                'rpt_dt']


    #to account for missing info on API return:
    for i in use_columns:
        if i not in nyc_crime_raw.columns:
            nyc_crime_raw[i] = ''
    # Rename columns
    rename_to = {'cmplnt_num':'Complaint_ID',
                'boro_nm':'Borough_Name',
                'cmplnt_fr_dt':'Complaint_Filed_Date',
                'cmplnt_to_dt':'Complain_End_Date',
                'hadevelopt':'Housing_Development_Name', 
                'law_cat_cd':'Offense_Severity',
                'loc_of_occur_desc':'Crime_Location_in_Building', 
                'pd_desc':'Crime_Description', 
                'prem_typ_desc':'LOCATION_TYPE', 
                'rpt_dt':'Report_Date'}

    nyc_crime_df = nyc_crime_raw[use_columns].rename(columns = rename_to)

    # Convert date fields to datetime type
    date_fields = ['Complaint_Filed_Date','Complain_End_Date','Report_Date']

    nyc_crime_df[date_fields] = nyc_crime_df[date_fields].apply(pd.to_datetime, errors='coerce')
    
    # Select only one start date for a a report as the date which came first: date crime reported, date complaint filed.
    nyc_crime_df['Start_Date'] = nyc_crime_df[['Complaint_Filed_Date','Report_Date']].apply(lambda x: min(x[0],x[1]), axis = 1)

    #Drop no longer neccessary columns
    nyc_crime_df.drop(['Complaint_Filed_Date','Report_Date'],axis = 1,inplace = True)
    
    all_caps_fields = ['Borough_Name', 'Housing_Development_Name','Offense_Severity', 'Crime_Location_in_Building', 'Crime_Description','LOCATION_TYPE']
    
    #Convert columns of text data that are in all caps to title case.
    nyc_crime_df[all_caps_fields] = nyc_crime_df[all_caps_fields]\
    .fillna('')\
    .applymap(lambda x: x.title())

    # Add additional metrics
    nyc_crime_df['Time_to_Resolution'] = nyc_crime_df['Complain_End_Date'] - nyc_crime_df['Start_Date']

    # Extract crime types using custom function (defined above)
    nyc_crime_df['Crime_Description'] = nyc_crime_df['Crime_Description'].apply(crime_type)

    # Convert all columns to lowercase.
    nyc_crime_df = nyc_crime_df.rename(columns = lambda x: x.lower())
    
    return nyc_crime_df