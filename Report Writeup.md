**1. What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?**

The goal of this project is to simulate data aggregation from several sources into one analytical table that can be used as part of a website UI for a housing business. This ELT combines crime data from NYC Open Data and Property Sales data from the NYC Department of Finance into an analytical table to be used to gauge neighborhood safety at a high level.

Both the crime data and housing data are prepared in the same set of steps. For both, the process is to download the data from its source online, then select columns that are chosen to be used, then columns are renamed. The data types are set manually for date columns and numeric type columns. Lastly, the cleaned dataframe is written to JSON using a custom writer. Once the data is in S3, the SQL queries run to CREATE "staging" tables for the raw JSON data. Then using `CREATE TABLE <table-name> AS`, the stage tables are used to create new tables where data is cleaned (trim whitespace), new metrics are added like indicator fields, and a join key is created using a select statement.

Lastly, the data from CRIMES and PROPERTY are joined on housing_key_name and combined into the table NYC_PROPERTY_AND_CRIME. The table NYC_PROPERTY_AND_CRIME is meant to be used as an analytics table for neighborhood-wide crime and property value statistics. This table could be used on a website like Zillow for "neighborhood safety".

This data model was chosen due to the fact that exploratory data analysis showed there were not enough rows or fields to join the crimes data to the property data into a meaningful Star schema model. 


Spark could be incorporated for two use cases. First, spark could be used if we wanted to do a spatial join between the two tables using city block-size geofences to join property sales to crime data more accurately and with more cardinality.

Airflow could be used to automate the whole process. In testing, I have been using a batch script to run all the steps in order and Airflow could be used instead if there was a desire to repeat the process often.

**2. Clearly state the rationale for the choice of tools and technologies for the project.**

It was chosen to use Redshift for this project due to the fact that there is not a large number of computationally expensive things, it was decided that SQL alone would fulfill all necessary steps. 

**3. Document the steps of the process.**

- Get Data
    - Download crimes data from NYC Open Data
    - Download property sales data from S3 from AWS Data Marketplace.

- Process Data
     - Rename columns, format types, and write data to JSON

- Upload downloaded data to S3

- Create a Redshift cluster.

- Run ELT process from S3 to Redshift.

**4. Propose how often the data should be updated and why.**

- The data should be updated once a month or even annually. The reason this data should be updated too often is because crime statistics are often biased by societal trends and news laws that can lead to rapid escalations of crime statistics. Similarly, the housing market can be rather volatile at times. Since, the end result of the data being the combined property values and crime statistics table, it makes sense to update those values rather infrequently as a way to ensure statistical consistency. 


**5. How the project would be different if:**
 - If the data was increased by 100x:
    - If the data was increased by 100x, in the preprocessing steps it would make sense to use many more smaller files rather than a few large files. Additionally, if the data increased by 100x it would make sense to change the SQL that creates the CRIMES and PROPERTY tables to filter only for valid rows. In the crime data, it would make sense to only look at misdemeanors and felonies. In the housing data, it would make sense to filter based on how recent a property was sold to ensure the values reflect the current market.

 - If the pipelines were run on a daily basis by 7am:
    - The steps of this process could be put into an Airflow DAG and scheduled to run.

 - If the database needed to be accessed by 100+ people:
    - If the database needed to be accessed by 100+ people, then a few things would need to happen. First, a read-only role would need to be created within Reshift for these users to use in Redshift. Additionally, we would want to make a new database user that uses that role and an IAM user for that database user profile linked to our AWS account. Depending on performance, it could make sense to increase the size of the Redshift clusters. In addition, to hide the staging tables with messy data it would be better to create another schema with read only privileges and store the CRIMES, PROPERTY, and NYC_PROPERTY_AND_CRIME tables in that public schema.
