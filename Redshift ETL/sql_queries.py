
import json
import os

cwd = os.getcwd()
keys_file = os.path.join(cwd, 'keys.json')
keys = json.load(open(keys_file))

DWH_ROLE_ARN = keys['Redshift']['RoleARN']
crime_json_path = keys['Redshift']['From_S3_Path']['Crime']['JSONPath']
housing_json_path = keys['Redshift']['From_S3_Path']['Housing']['JSONPath']

housing_data_path = keys['Redshift']['From_S3_Path']['Housing']['Data']
crime_data_path = keys['Redshift']['From_S3_Path']['Crime']['Data']

drop_crimes_staging = "DROP TABLE IF EXISTS STAGING_NYC_CRIME;"
drop_property_staging = "DROP TABLE IF EXISTS STAGING_NYC_PROPERTY_SALES;"
drop_crimes_table = "DROP TABLE IF EXISTS CRIMES;"
drop_properties_table = "DROP TABLE IF EXISTS PROPERTIES;"
drop_analysis_table = "DROP TABLE IF EXISTS NYC_PROPERTY_AND_CRIME;"



#############################################################
################    CREATE TABLE QUERIES     ################            
#############################################################
create_table_crime_raw = """CREATE TABLE STAGING_NYC_CRIME
(CRIME_ID INT IDENTITY(1, 1),
BOROUGH_NAME VARCHAR,
COMPLAINT_ID VARCHAR,
COMPLAIN_END_DATE NUMERIC,
HOUSING_DEVELOPMENT_NAME VARCHAR,
OFFENSE_SEVERITY VARCHAR,
CRIME_LOCATION_IN_BUILDING VARCHAR,
CRIME_DESCRIPTION VARCHAR,
LOCATION_TYPE VARCHAR,
START_DATE NUMERIC,
TIME_TO_RESOLUTION NUMERIC);"""




create_table_property_sales_raw = """CREATE TABLE STAGING_NYC_PROPERTY_SALES
(PROPERTY_ID INT IDENTITY(1, 1),
COMMERCIAL_UNITS INT,
GROSS_SQUARE_FEET NUMERIC,
RESIDENTIAL_UNITS NUMERIC,
SALE_PRICE NUMERIC,
BOROUGH_NUMBER INT,
TOTAL_UNITS INT,
BLOCK INT,
ZIP_CODE VARCHAR,
SALE_DATE DATE,
NEIGHBORHOOD VARCHAR,
LAND_SQUARE_FEET NUMERIC,
EASEMENT VARCHAR,
LOT VARCHAR,
BOROUGH_NAME VARCHAR,
YEAR_BUILT INT,
TAX_CLASS_AT_TIME_OF_SALE VARCHAR,
APARTMENT_NUMBER VARCHAR,
ADDRESS VARCHAR,
BUILDING_CLASS_AT_TIME_OF_SALE VARCHAR,
BUILDING_CLASS_CODE VARCHAR,
BUILDING_CLASS_TYPE VARCHAR,
BUILDING_CLASS_DETAILS VARCHAR);"""





#############################################################
################    COPY INTO QUERIES        ################            
#############################################################

copy_property_data = """COPY STAGING_NYC_PROPERTY_SALES(
    COMMERCIAL_UNITS,
    GROSS_SQUARE_FEET,
    RESIDENTIAL_UNITS,
    SALE_PRICE,
    BOROUGH_NUMBER,
    TOTAL_UNITS,
    BLOCK,
    ZIP_CODE,
    SALE_DATE,
    NEIGHBORHOOD,
    LAND_SQUARE_FEET,
    EASEMENT,
    LOT,
    BOROUGH_NAME,
    YEAR_BUILT,
    TAX_CLASS_AT_TIME_OF_SALE,
    APARTMENT_NUMBER,
    ADDRESS,
    BUILDING_CLASS_AT_TIME_OF_SALE,
    BUILDING_CLASS_CODE,
    BUILDING_CLASS_TYPE,
    BUILDING_CLASS_DETAILS) 
FROM '{}'
iam_role '{}'
format as json '{}'
dateformat AS 'YYYY-MM-DD'
STATUPDATE ON
region 'us-west-2';
""".format(housing_data_path, DWH_ROLE_ARN, housing_json_path)


copy_crime_data = """COPY STAGING_NYC_CRIME(
    BOROUGH_NAME,
    COMPLAINT_ID,
    COMPLAIN_END_DATE,
    HOUSING_DEVELOPMENT_NAME,
    OFFENSE_SEVERITY,
    CRIME_LOCATION_IN_BUILDING,
    CRIME_DESCRIPTION,
    LOCATION_TYPE,
    START_DATE,
    TIME_TO_RESOLUTION)
FROM '{}'
iam_role '{}'
format as json '{}'
STATUPDATE ON
region 'us-west-2';
""".format(crime_data_path, DWH_ROLE_ARN, crime_json_path)




make_crimes_table = """CREATE TABLE CRIMES AS
SELECT 
CRIME_ID,
UPPER(TRIM(HOUSING_DEVELOPMENT_NAME)) AS HOUSING_NAME_KEY,
COALESCE(TRIM(BOROUGH_NAME),'Not Reported') AS BOROUGH_NAME,
COALESCE(TRIM(HOUSING_DEVELOPMENT_NAME),'Not Reported') AS HOUSING_DEVELOPMENT_NAME,
COALESCE(LOCATION_TYPE,'Not Reported') AS LOCATION_TYPE, 
COALESCE(CRIME_DESCRIPTION, 'Not Classified') AS CRIME_TYPE,
CAST(TIMESTAMP 'epoch' + START_DATE/1000::FLOAT *INTERVAL '1 second' AS DATE)  AS REPORTED_DATE,
CAST(TIME_TO_RESOLUTION IS NULL AS INT) AS IS_SOLVED,
CAST(OFFENSE_SEVERITY = 'Violation' AS INT) AS CIVIL_VIOLATION_IND,
CAST(OFFENSE_SEVERITY = 'Misdemeanor' AS INT) AS MISDEMEANOR_IND,
CAST(OFFENSE_SEVERITY = 'Felony' AS INT) AS FELONY_IND
FROM STAGING_NYC_CRIME;
"""


make_property_table = """CREATE TABLE PROPERTY AS
SELECT 
PROPERTY_ID, 
UPPER(TRIM(NEIGHBORHOOD)) AS HOUSING_NAME_KEY,
BOROUGH_NAME,
SUBSTRING(TRIM(ZIP_CODE),1,5) AS ZIPCODE,
CAST(BLOCK AS VARCHAR) AS BLOCK_CODE,
TRIM(NEIGHBORHOOD) AS NEIGHBORHOOD_NAME,
TRIM(ADDRESS) AS ADDRESS,
APARTMENT_NUMBER,
CASE
WHEN YEAR_BUILT = 0 THEN NULL
ELSE EXTRACT(YEAR FROM CURRENT_DATE) - YEAR_BUILT 
END AS YEARS_SINCE_BUILT,
BUILDING_CLASS_TYPE AS PROPERTY_CLASS,
BUILDING_CLASS_DETAILS AS PROPERTY_CLASS_TYPE,
TOTAL_UNITS AS NUM_PROPERTIES,
COMMERCIAL_UNITS AS NUM_COMMERCIAL_PROPERTIES,
RESIDENTIAL_UNITS AS NUM_RESIDENTIAL_PROPERTIES,
SALE_DATE,
EXTRACT(YEAR FROM SALE_DATE) AS LAST_SALE_YEAR,
DATEDIFF(year,SALE_DATE, CURRENT_DATE) AS YEARS_SINCE_LAST_SALE,
GROSS_SQUARE_FEET,
LAND_SQUARE_FEET,
SALE_PRICE,
CASE 
WHEN GROSS_SQUARE_FEET = 0 THEN NULL
ELSE SALE_PRICE/GROSS_SQUARE_FEET END AS PRICE_PER_SQ_FOOT
FROM STAGING_NYC_PROPERTY_SALES;"""

make_analysis_table = """CREATE TABLE NYC_PROPERTY_AND_CRIME AS
SELECT 
PROPERTY.BOROUGH_NAME,
HOUSING_DEVELOPMENT_NAME, 
ZIPCODE,
NEIGHBORHOOD_NAME,
LOCATION_TYPE,
PROPERTY_CLASS,
CRIME_TYPE,
SUM(NUM_COMMERCIAL_PROPERTIES) AS COUNT_BUSINESS_UNITS,
SUM(NUM_RESIDENTIAL_PROPERTIES) AS COUNT_HOUSING_UNITS,
AVG(CIVIL_VIOLATION_IND) AS PERCENT_CIVIL_VIOLATIONS,
AVG(MISDEMEANOR_IND) AS PERCENT_MISDEMEANORS,
AVG(FELONY_IND) AS PERCENT_FELONIES,
AVG(IS_SOLVED) AS CRIME_RESOLUTION_RATE,
AVG(SALE_PRICE) AS AVERAGE_HOUSING_COST,
SUM(GROSS_SQUARE_FEET) AS LAND_AREA,
AVG(YEARS_SINCE_BUILT) AS AVERAGE_NEIGHBORHOOD_AGE
FROM CRIMES
JOIN PROPERTY
ON CRIMES.HOUSING_NAME_KEY = PROPERTY.HOUSING_NAME_KEY
GROUP BY 
PROPERTY.BOROUGH_NAME,
HOUSING_DEVELOPMENT_NAME, 
ZIPCODE,
NEIGHBORHOOD_NAME,
LOCATION_TYPE,
PROPERTY_CLASS,
CRIME_TYPE;"""

proc = {
    'Staging':{
        'Drop Old': [
            drop_crimes_staging,
            drop_property_staging
            ],
            
        'Create New': [
            create_table_crime_raw,
            create_table_property_sales_raw
            ],
        'Copy Into': [
            copy_crime_data,
            copy_property_data
            ]       
    },
    'Analysis':{
        'Drop Old': [
            drop_crimes_table,
            drop_properties_table,
            drop_analysis_table

        ],
        'Create Table As':[
            make_crimes_table,
            make_property_table,
            make_analysis_table
        ]
    }
}
