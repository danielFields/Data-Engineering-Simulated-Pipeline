# Data Engineering Capstone Project
This project is a simulated data engineering problem solution using crimes data and property sales data in New York City, USA.

**This ELT combines crime data from NYC Open Data and Property Sales data from the NYC Department of Finance into an analytical table to be used to gauge neighborhood safety at a high level.**
 
## Data Sources
### NYC Crimes Data
The crime data is pulled from NYC Open Data API. The dataset has the unique identifier number of `qgea-i56i`.
 
The crime data can be found and downloaded from [this link.](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i)
 
The crime data consists of records of police criminal complaints with each row being a unique complaint. In total, 7.38M Rows by 35 Columns in all for years 2006-2019.
 
In the scope of this project, only the years 2017,2018, and 2019 were used resulting in a dataset of 1,394,764 Rows.
 
### NYC Property Sales Data
The NYC Property Sales data was gathered from the AWS Data Marketplace. This data is provided for free by the [NYC Department of Finance](https://www1.nyc.gov/site/finance/taxes/property-annualized-sales-update.page). The data ranges in time from 2014-2018, and the years of 2016-2018 were used in this project.
 
In total, this data as put into S3 for consumption into Redshift contains 262,248 Rows and 22 columns. THe raw data contains the below fields, and all are used.
 
|column                                |data type|
|--------------------------------------|---------|
|borough_name                          | object  |
|borough                               | int64   |
|neighborhood                          | object  |
|building_class_category               | object  |
|tax_class_as_of_final_roll_YY/YY      | object  |
|block                                 | int64   |
|lot                                   | int64   |
|ease-ment                             | object  |
|building_class_as_of_final_roll_YY/YY | object  |
|address                               | object  |
|apartment_number                      | object  |
|zip_code                              | int64   |
|residential_units                     | object  |
|commercial_units                      | int64   |
|total_units                           | object  |
|land_square_feet                      | object  |
|gross_square_feet                     | object  |
|year_built                            | int64   |
|tax_class_at_time_of_sale             | int64   |
|building_class_at_time_of_sale        | object  |
|sale_price                            | object  |
|sale_date                             | object  |
 
## Data Preprocessing
 
Both the crime data and housing data are prepared in the same set of steps. For both, the process is to download the data from its source online, then select columns that are chosen to be used, then columns are renamed. The data types are set manually for date columns and numeric type columns. Lastly, the _cleaned_ dataframe is written to JSON using a custom writer.
 
The crime data preprocessing scripts are in `NYC-Crime` and the property sales datasets are in `NYC-Housing`.
 
The reason that a custom writer is needed is because of the mismatch between the prebuilt `pandas.DataFrame.to_json()` function's output and the form of JSON expected by Redshift for `COPY INTO`.
 
For example, if one had the dataframe:
 
|Column1    |Column2    |ColumnN    |
|-----------|-----------|-----------|
|Row1Column1|Row1Column2|Row1ColumnN|
|RowNColumn1|RowNColumn2|RowNColumnN|
 
And, used `pandas.DataFrame.to_json()` to save the data before uploading to the cloud, the JSON data would be like:
`[{"Column1":"Row1Column1","Column2":"Row1Column2","ColumnN":"Row1ColumnN"},{"Column1":"RowNColumn1","Column2":"RowNColumn2","ColumnN":"RowNColumnN"}]`
 
The issue is that Redshift expects new lines between each observation so Reshift sees the above JSON as one element only. This leads to an error that the copy input is larger than the max allowable size.
 
To Fix this, a custom function was written which instead would yield:
`{"Column1":"Row1Column1","Column2":"Row1Column2","ColumnN":"Row1ColumnN"}`
`{"Column1":"RowNColumn1","Column2":"RowNColumn2","ColumnN":"RowNColumnN"}`
 
## ELT
 
Once the data is in S3, then the scripts in `Redshift ETL` sets up a data warehouse in Redshift.
 
The script `Redshift ETL\provision_redshift.py` creates a redshift cluster. Then, `Redshift ETL\data_to_redshift.py` will allocate JSON data from S3 to Redshift sequentially.
 
First, staging tables for crime data,`STAGING_NYC_CRIME`, and property sales data, ,`STAGING_NYC_PROPERTY_SALES` are created and the raw JSON data from S3 is copied in using the `COPY INTO` command and a custom jsonpath file (uploaded to S3 as well). Both the Crimes Data and Property Data have their jsonpath files stored in their directories in this repo for demonstration.
 
Then, the raw crimes data is `STAGING_NYC_CRIME` is used to make the `CRIMES` table which creates a few indicator columns, trims whitespace on character columns, and creates the _primary key_ used in the table, `HOUSING_NAME_KEY`, which is the neighborhood name in all capitals.
 
Similarly, the raw property sales data is `STAGING_NYC_PROPERTY_SALES` is used to make the `PROPERTY` table which creates a few new metrics like time since last sale, trims whitespace on character columns, and creates the _primary key_ used in the table, `HOUSING_NAME_KEY`, which is the neighborhood name in all capitals.
 
Finally, the data from `CRIMES` and `PROPERTY` are joined on `housing_key_name` and combined into the table `NYC_PROPERTY_AND_CRIME`. The table `NYC_PROPERTY_AND_CRIME` is meant to be used as an analytics table for neighborhood-wide crime and property value statistics. This table could be used on a website like Zillow for "neighborhood safety".
 
Additionally, `CRIMES` and `PROPERTY` tables could be combined with other data like weather data or economic data.



## Process Checks
After the ETL Script `Redshift ETL\data_to_redshift.py` has run, the final step is to run the `Redshift ETL\checks.py` scipt that will check that the ETL process worked correctly. The `checks.py` script will connect to the database cluster and for each table in the database that should be there ("STAGING_NYC_PROPERTY_SALES","STAGING_NYC_CRIME", "CRIMES","PROPERTY","NYC_PROPERTY_AND_CRIME") exist and have data in them.


# Data Dictionary
![Raw Property Sales Data (Stage Data) data dictionary.](/Data-Dictionary/STAGING_NYC_PROPERTY_SALES.PNG)
![Cleaned Property Sales Data with new fields added data dictionary.](/Data-Dictionary/PROPERTY.PNG)


![Raw Crimes Data (Stage Data) data dictionary.](/Data-Dictionary/STAGING_NYC_CRIME.PNG)
![Cleaned Crimes Data with new fields added data dictionary.](/Data-Dictionary/CRIMES.PNG)


Then, **CRIMES** and **PROPERTY** join together via `HOUSING_NAME_KEY` and are aggregated into.

![Aggregated Analysis Table with Property values and crime statistics data dictionary.](/Data-Dictionary/NYC_PROPERTY_AND_CRIME.PNG)








