# US Immigration - Data Analysis
### Data Engineering Capstone Project
#### Created by Nicolas Nouchi

#### Project Summary

In this project, we will be analyzing the immigration volumes in US cities by month and year. This includes creating an ETL pipeline that includes assessing the initial data from CSV files and text files. The data will be transformed into clean, actionable datasets. 

This information can be used by a government department/agency or an airline company to understand the distribution of visitors within a city, allowing them to make improvements of the infrastructure of their airports or anticpate the need for further housing developments.

This information is distributed between Amazon Web Services tools **S3 and Redshift**.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up





## Step 1: Scope the Project and Gather Data

### Scope
- **Scope of the Project**
    - In this project, I have planned to analyze the immigration volumes in the US cities by month and year. The data I plan on using is listed below (**Data Selection**) and will utilize the pandas library to clean the CSV & TXT files. Then I will import these cleaned csv files into S3 and reference them to create my tables in Redshift.  

## Describe and Gather Data 

- **Data Selection**
    - I94 Immigration Data: This data comes from the US National Tourism and Trade Office. It includes various information regarding the year, month, port, birth year and more of an individual.

    - World Temperature Data: This dataset came from Kaggle. It includes various information regarding the average temperature, year, city and country.

    - U.S. City Demographic Data: This data comes from OpenSoft. It includes various information regarding the city, state, median age, total population and veterans.

    - Airport Code Table: This is a simple table of airport codes and corresponding cities. It includes various information regarding the type of airport, the elevation, iso_region and name of the airport.

- **SAS Labels Descriptions**
    - I94_ADDRL: This includes states and territories from the United States
    - I94_CNTYL: This includes countries and their country code (i.e. i94res from the immigration table)  
    - I94_MODE: This includes transportation modes of travel and their designated code (i.e. i94mode from the immigration table)
    - I94_PORT: This includes port names and codes (i.e. i94port from the immigration table)
    - I94_VISA: This includes visa codes and their type (i.e. i94visa from the immigration table) 




### Step 2: Explore and Assess the Data
#### Explore the Data 
Identify data quality issues, like missing values, duplicate data, etc.

#### Cleaning Steps
Document steps necessary to clean the data




**3.1**

The best model to perform high-value analysis in this situation would be to utilize a Star Schema. 

This model would focus on utilizing one fact table *i94_immigration_data* and multiple dimensional tables including us_demographic_data, airport_data, global_temp_data, i94_cntyl_data, i94_port_data, i94_mode_data, i94_addr_data, and i94_visa_data.

The flexibility is there for these tables to be utilized in additional analysis that may be required beyond the fact table, therefore if needed, one could join together these tables. Then they can utilize the information necessary such as how many people traveled to a given city within a period of months or years.



### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Build the data pipelines to create the data model.


#### 4.2 Data Quality Checks


- 1) Ensuring that the primary keys of the tables align with the source data, I will compare the number of unique rows and if they are equivalent, then the check will show that there are correct constraints.
- 2) The original dataframe should show for all tables that have a primary key, that the unique values align with the total count of the primary key. This is done by the function **check_unique_counts** that compares the original dataframe from the csv file and the table hosted in Redshift.
- 3) Checking datatypes of the tables created through Redshift to determine if they kept their types through the ETL process

 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness
 
Run Quality Checks
The quality checks that will determine whether the pipeline ran successfully includes verifying that the tables located in Redshift are populated **(i.e. the function below check_tables)** and verifying that the count of unique keys are the same compared to the initial dataframe created prior to their final location in **SAS_dimensions_cleaned**.


**Dimension table : global_temp_data**
- dt: Date that the average temperature was recorded | VARCHAR
- AverageTemperature: Average temperature of the day | FLOAT
- AverageTemperatureUncertainty: Average uncertainty of temperature | FLOAT
- City: City name of the record | VARCHAR
- Country: Country name of the record | VARCHAR
- Longitude: Latitude of the city | FLOAT
- Latitude: Longitude of the city | FLOAT

**Dimension table : us_demographic_data**
- city: City name used to represent the corresponding demographic information | VARCHAR
- state: State name of the city | VARCHAR
- median_age: Median age of the city | FLOAT
- male_population: Total count of male population of the city | FLOAT
- female_population: Total count of the female population of the city | FLOAT
- total_population: Total population of the city | INTEGER
- foreign_born: Total foreign born population of the city | FLOAT
- avg_household_size: Average household size of the city | FLOAT
- state_code: State code of the city | VARCHAR | Foreign key to code from i94_addr_data
- race: Category of individual in the record | VARCHAR
- count: Record count of the population by race | INTEGER

**Dimension table : airport_data**
- ident: Identify the corresponding airport code used in the immigration data | VARCHAR PRIMARY KEY
- type: Type of airport | VARCHAR
- name: Name of the airport | VARCHAR
- elevation_ft: Elevation of the airport | VARCHAR
- continent: Continent of the airport | VARCHAR
- iso_country: ISO Country code of the airport | VARCHAR
- iso_region: ISO Region code of the airport | VARCHAR
- municipality: Municipality of the airport | VARCHAR
- gps_code: GPS Code of the airport | VARCHAR
- iata_code: IATA Code used to determine the name of the airport | VARCHAR
- local_code: Local code of the airport | VARCHAR
- latitude: Latitude of the airport | FLOAT
- longitude: Longitude of the airport | FLOAT


**Dimension table : i94_addr_data**
- code: Code that corresponds to the state or territory code | VARCHAR PRIMARY KEY
- addr: Name of the state of territory code | VARCHAR

**Dimension table : i94_mode_data**
- code: Code that corresponds to the mode of transportation | VARCHAR PRIMARY KEY
- mode: Name of the mode of transportation | VARCHAR

**Dimension table : i94_port_data**
- code:  Code that maps the city/country/location | VARCHAR PRIMARY KEY
- port: Name of the port that maps city/country/location | VARCHAR

**Dimension table : i94_cntyl_data**
- code: Maps the country code back to the i94_immigration_data table | VARCHAR PRIMARY KEY
- country: Name of the country code | VARCHAR

**Dimension table : i94_visa_data**
- code: Primary key that is used to determine type of visa code | VARCHAR PRIMARY KEY
- type: Type of visa | VARCHAR

**Fact table : i94_immigration_data**
- cicid: ID | VARCHAR
- i94yr: year | VARCHAR
- i94mon: Numeric month | VARCHAR
- i94cit: Cities code | VARCHAR
- i94res: Countries code | VARCHAR | Foreign key to code from i94_cntyl_data
- i94port: Ports code | VARCHAR | Foreign key to code from i94_port_data
- arrdate: Arrival date | VARCHAR
- i94mode: Mode of transportation code | VARCHAR | Foreign key to code from i94_mode_data
- i94addr: State or territory code | VARCHAR | Foreign key to code from i94_addr_data
- depdate: Departure date | VARCHAR
- i94bir: Age | VARCHAR
- i94visa: Visa count | VARCHAR | Foreign key to code from i94_visa_data
- count: Record count | VARCHAR
- visapost : Visa post | VARCHAR
- biryear : Birth year | VARCHAR
- gender : Gender | VARCHAR
- airline : Airline type | VARCHAR
- visatype : Visa type | VARCHAR



#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.

My choice of tools being heavily focused on utilizing the pandas library in Python helped make data exploration and transformation possible in my project.

The decision I made was to do as much of the heavy lifting in the beginning through cleaning and transforming the dataframes, allowing a smooth transition to create the tables and load the data from S3 to Redshift.

Spark was utilized in the background processes such as reading the SAS data in the initial exploration.

* Propose how often the data should be updated and why.

The data would be best updated on a monthly basis to ensure it aligns with the fact table **i94_immigration_data**. This would alleviate the workload required to moving the data into S3 on a daily or weekly basis.

* Write a description of how you would approach the problem differently under the following scenarios:

 * The data was increased by 100x.
 
    - The scenario would require a change of tools, with the option of utilizing Spark or an Amazon EMR instance for parallel processing capability. This would help speed up ETL processes by distributing them among different nodes and simplify the process to allow for scaling up or down depending on the size of the data.
 
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
    - The scenario would be best fit for Airflow, as it would allow for automatic scheduling and monitoring
 
 * The database needed to be accessed by 100+ people.
    - I believe Redshift would be the appropriate tool in this scenario as it allows easy access to query directly with a useful interface and the least amount of configuration.
