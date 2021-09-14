US Immigration - Data Analysis
Data Engineering Capstone Project
Created by Nicolas Nouchi
Project Summary
In this project, we will be analyzing the immigration volumes in US cities by month and year. This includes creating an ETL pipeline that includes assessing the initial data from CSV files and text files. The data will be transformed into clean, actionable datasets.

This information can be used by a government department/agency or an airline company to understand the distribution of visitors within a city, allowing them to make improvements of the infrastructure of their airports or anticpate the need for further housing developments.

This information is distributed between Amazon Web Services tools S3 and Redshift.

The project follows the follow steps:

Step 1: Scope the Project and Gather Data
Step 2: Explore and Assess the Data
Step 3: Define the Data Model
Step 4: Run ETL to Model the Data
Step 5: Complete Project Write Up
In [1]:
# Do all imports and installs here
import pandas as pd
import configparser
import psycopg2
import boto3
#from sql_queries import create_table_queries, drop_table_queries, copy_table_queries
In [2]:
#read data
fname='../../data2/GlobalLandTemperaturesByCity.csv'
global_temp_df = pd.read_csv(fname)
In [3]:
global_temp_df.head()
Out[3]:
dt	AverageTemperature	AverageTemperatureUncertainty	City	Country	Latitude	Longitude
0	1743-11-01	6.068	1.737	Århus	Denmark	57.05N	10.33E
1	1743-12-01	NaN	NaN	Århus	Denmark	57.05N	10.33E
2	1744-01-01	NaN	NaN	Århus	Denmark	57.05N	10.33E
3	1744-02-01	NaN	NaN	Århus	Denmark	57.05N	10.33E
4	1744-03-01	NaN	NaN	Århus	Denmark	57.05N	10.33E
In [4]:
fname = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
immigration_df = pd.read_sas(fname, 'sas7bdat', encoding="ISO-8859-1")
immigration_df.head()
Out[4]:
cicid	i94yr	i94mon	i94cit	i94res	i94port	arrdate	i94mode	i94addr	depdate	...	entdepu	matflag	biryear	dtaddto	gender	insnum	airline	admnum	fltno	visatype
0	6.0	2016.0	4.0	692.0	692.0	XXX	20573.0	NaN	NaN	NaN	...	U	NaN	1979.0	10282016	NaN	NaN	NaN	1.897628e+09	NaN	B2
1	7.0	2016.0	4.0	254.0	276.0	ATL	20551.0	1.0	AL	NaN	...	Y	NaN	1991.0	D/S	M	NaN	NaN	3.736796e+09	00296	F1
2	15.0	2016.0	4.0	101.0	101.0	WAS	20545.0	1.0	MI	20691.0	...	NaN	M	1961.0	09302016	M	NaN	OS	6.666432e+08	93	B2
3	16.0	2016.0	4.0	101.0	101.0	NYC	20545.0	1.0	MA	20567.0	...	NaN	M	1988.0	09302016	NaN	NaN	AA	9.246846e+10	00199	B2
4	17.0	2016.0	4.0	101.0	101.0	NYC	20545.0	1.0	MA	20567.0	...	NaN	M	2012.0	09302016	NaN	NaN	AA	9.246846e+10	00199	B2
5 rows × 28 columns
