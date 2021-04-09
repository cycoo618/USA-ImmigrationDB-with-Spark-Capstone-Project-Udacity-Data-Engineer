# USA-ImmigrationDB-with-Spark-Capstone-Project-Udacity-Data-Engineer
##### _Capstone Project of Data Engineering Nanodegree in Udacity_
This project will create a data model for immigration data, and will build a data pipeline to perform the ETL process in spark which writes parquet file as output.

_Note: Giving the huge size of the immigration data, only a subset of the data was loaded here._
## Project Summary
This project will create a data model for immigration data, and will build a data pipeline to perform the ETL process in spark which writes parquet file as output

## Files:
- `Capstone Project Final.ipynb`: main file for pre-processing and ETL pipeline with explanations
- `Analysis in Pandas.ipynb`: exploratory analysis after the ETL in Pandas
- `Analysis in SQL.ipynb`: exploratory analysis after the ETL in SQL


## The project follows the follow steps:
* Step 0: Scope the Project
* Step 1: Gather, Explore and Clean the Data
* Step 2: Define the Data Model
* Step 3: Run ETL to Model the Data
* Step 4: Complete Project Write Up

### 1. Scope 
##### 1.1 Data will be used: 

- immigration data: immigration_data_sample.csv
    - Contains information about immigration details
    - This data comes from the US National Tourism and Trade Office (https://travel.trade.gov/research/reports/i94/historical/2016.html).
    - This is a small subset of the immigration data
     
     
- i94 description file for immigrasion data: I94_SAS_Labels_Descriptions.sas
    - This is a file contains the descriptions and references for the codes being used in the immigration dataset. 


- demographic data: us-cities-demographics.csv
    - Contains information about demographic data based on State-City pairs
    - This data comes from OpenSoft (https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
     


- downloaded dataset: /downloaded/FCDO_Geographical_Names_Index.csv
    - Contains country name and 2-digit code, and other information about the country. This is to complement the i94 description data.
    - This data comes from nationsonline.org (https://www.nationsonline.org/oneworld/country_code_list.htm).

##### 1.2 End solution:

A Data Model with a fact table that has immigration information and a series of dimension table with further details


##### 1.3 Tools will be used:

 - One time pre-processing: Pandas
 - ETL pipeline: Spark
 - Storage: S3 bucket (this is not in the submitted version since it will take an extremely long time to write and read using S3. The submitted project only use local storage for storing the parquet files)

### 2. Schema
#### Fact table:
##### immigration
 - cicid: primary key, unique ID for each visiting record
 - cit_ctry: the country code the visitor's/immigrant's citizenship belongs to
 - res_ctry: the country code the visitor's/immigrant's residence belongs to
 - trnps_mode_code: the transport mode code for the visitor/immigrant coming to U.S.
 - address_state: visiting State
 - arrival_date: date of arrival at U.S.
 - depart_date: date of departure from U.S.
 - age: visitor's age
 - birth_year: visitor's birth year
 - dpmt_visa: department issued the visa
 - occupation: visitor/immigrant's occupation in U.S.
 - visa_expiry_date: visa's expiry date
 - gender: visitor's/immigrant's gender
 - airline: the airline company the visitor/immigrant took to U.S.
 - port_code: port information of landing
 - visa_type: type of visa was legally admitted for visiting U.S.

#### Dimension table:
##### demographic
- city
- state_code: 2-letter State code
- state: state full name
- median_age: median age within the city in the state
- male_pplt: male population
- female_pplt: female population
- total_pplt: total population
- veteran_pplt: the number of veteran
- foreign_born: the number of foreign born
- average_household_size: average house hold size in the city in the state
- largest_race: largest race in the city in the state
- largest_race_pplt: the population of the largest race in the city in the state
        
##### state_race: list all the races in each city in each state
- city
- state_code
- state
- race
- count: population for the specific race
- order: the descending order (from the largest to smallest) of the population for the specific race in the city in the state

##### temperature: list the daily temperature for each city in each state for year 2004 ~ 2013
- date
- city
- country
- avg_temperature
- avg_temperature_uncertainty
- latitude
- longitude

##### time
- date
- month
- day
- weekday
- year

##### country
- code: i94 country code, which is the code used in the immigration data
- country: 2-letter country code
- name: country name
- official_name: the full name of the country
- citizen_name: the citizen's name for that country
- continent

##### port
- code: i94 port code, which is the code used in the immigration data
- address: address contains city and state in the sas description file
- city: parsed city
- state: parsed state

##### mode: transportation code
- code: i94 transportation code being used in the immigration data
- mode

##### visa
- code: i94 visa code being used in the immigration data
- visa


### 3. Improvements needed:

- Temperature table: has only city and country, but not state information. There are duplicated city names but within different states in U.S, and in fact, we have same city names but different state names in our dataset. In order to have better and more accurate analysis, state information needs to be added into the temperature table
- Also, the temperature table needs to be updated for a couple of more years' data. The year as of this project right now is 2021, and we are missing almost 8 years data. The immigration data is based on 2016, so there was a big off as well.

### 4. Analysis

There are two separate files for further analysis based on this data model

- Analysis in Pandas: Exploratory Analysis using Pandas, which was analyzing a subset of the dataset
- Analysis in SQL: Exploratory Analysis using Spark SQL, which was analyzing the full dataset of the immigration data. This file has fewer analysis.
