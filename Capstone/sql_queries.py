import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dl.cfg')

#Set the IAM_Role & S3 locations of the cleaned up files
IAM_ROLE = config['IAM_ROLE']['ARN']
US_DEMOGRAPHIC = config['S3']['US_DEMOGRAPHIC_DATA']
AIRPORT = config['S3']['AIRPORT_DATA']
GLOBAL_TEMP = config['S3']['GLOBAL_TEMP_DATA']
IMMIGRATION = config['S3']['IMMIGRATION_DATA']
I94_ADDR = config['S3']['I94_ADDR_DATA']
I94_CNTYL = config['S3']['I94_CNTYL_DATA']
I94_MODE = config['S3']['I94_MODE_DATA']
I94_PORT = config['S3']['I94_PORT_DATA']
I94_VISA = config['S3']['VISA_DATA']




# DROP TABLES

us_demographic_data_drop = "DROP TABLE IF EXISTS us_demographic_data;"
airport_data_drop = "DROP TABLE IF EXISTS airport_data;"
global_temp_data_drop = "DROP TABLE IF EXISTS global_temp_data;"
i94_immigration_drop = "DROP TABLE IF EXISTS i94_immigration_data;"
i94_addr_data_drop = "DROP TABLE IF EXISTS i94_addr_data;"
i94_cntyl_data_drop = "DROP TABLE IF EXISTS i94_cntyl_data;"
i94_mode_data_drop = "DROP TABLE IF EXISTS i94_mode_data;"
i94_port_data_drop = "DROP TABLE IF EXISTS i94_port_data;"
i94_visa_data_drop = 'DROP TABLE IF EXISTS i94_visa_data;'

# CREATE TABLES
i94_immigration_table = ("""CREATE TABLE IF NOT EXISTS i94_immigration_data (
                            cicid VARCHAR PRIMARY KEY,
                            i94yr VARCHAR,
                            i94mon VARCHAR,
                            i94cit VARCHAR,
                            i94res VARCHAR,
                            i94port VARCHAR,
                            arrdate VARCHAR,
                            i94mode VARCHAR,
                            i94addr VARCHAR,
                            depdate VARCHAR,
                            i94bir VARCHAR,
                            i94visa VARCHAR,
                            count VARCHAR,
                            visapost VARCHAR,
                            biryear VARCHAR,
                            gender VARCHAR,
                            airline VARCHAR,
                            visatype VARCHAR
                            )
""")

us_demographic_table = ("""CREATE TABLE IF NOT EXISTS us_demographic_data (
                  city VARCHAR,
                  state VARCHAR,
                  median_age FLOAT,
                  male_population FLOAT,
                  female_population FLOAT,
                  total_population INTEGER,
                  num_of_vets FLOAT,
                  foreign_born FLOAT,
                  avg_household_size FLOAT,
                  state_code VARCHAR,
                  race VARCHAR,
                  count INTEGER
                  )
""")

airport_table = ("""CREATE TABLE IF NOT EXISTS airport_data (
                         ident VARCHAR PRIMARY KEY,
                         type VARCHAR,
                         name VARCHAR,
                         elevation_ft VARCHAR,
                         continent VARCHAR,
                         iso_country VARCHAR,
                         iso_region VARCHAR,
                         municipality VARCHAR,
                         gps_code VARCHAR,
                         iata_code VARCHAR,
                         local_code VARCHAR,
                         latitude FLOAT,
                         longitude FLOAT
                         )
""")

global_temp_table = ("""CREATE TABLE IF NOT EXISTS global_temp_data (
                       dt VARCHAR,
                       AverageTemperature FLOAT,
                       AverageTemperatureUncertainty FLOAT,
                       City VARCHAR,
                       Country VARCHAR,
                       Longitude VARCHAR,
                       Latitude VARCHAR
                       )
""")

# INSERT RECORDS
us_demographic_table_insert = ("""COPY us_demographic_data
                           FROM {}
                           credentials 'aws_iam_role={}'
                           region as 'us-west-2'
                           IGNOREHEADER 1
                           format as csv;
                           """).format(US_DEMOGRAPHIC, IAM_ROLE)

i94_immigration_insert = ("""COPY i94_immigration_data
                           FROM {}
                           credentials 'aws_iam_role={}'
                           region as 'us-west-2'
                           IGNOREHEADER 1
                           format as csv;
                           """).format(IMMIGRATION, IAM_ROLE)

airport_table_insert = ("""COPY airport_data
                           FROM {}
                           credentials 'aws_iam_role={}'
                           region as 'us-west-2'
                           IGNOREHEADER 1
                           format as csv;
                           """).format(AIRPORT, IAM_ROLE)


global_temp_insert = ("""COPY global_temp_data
                           FROM {}
                           credentials 'aws_iam_role={}'
                           region as 'us-west-2'
                           IGNOREHEADER 1
                           format as csv;
                           """).format(GLOBAL_TEMP, IAM_ROLE)
                                       

#Dimension tables 
i94_cntyl_table = ("""CREATE TABLE IF NOT EXISTS i94_cntyl_data (
                       code VARCHAR PRIMARY KEY,
                       country VARCHAR
                       )
""")
i94_cntyl_insert = ("""COPY i94_cntyl_data 
                           FROM {}
                           credentials 'aws_iam_role={}'
                           region as 'us-west-2'
                           IGNOREHEADER 1
                           format as csv;
                           """).format(I94_CNTYL, IAM_ROLE)

i94_port_table = ("""CREATE TABLE IF NOT EXISTS i94_port_data (
                    code VARCHAR PRIMARY KEY,
                    port VARCHAR
                    )
""")

i94_port_insert = ("""COPY i94_port_data
                           FROM {}
                           credentials 'aws_iam_role={}'
                           region as 'us-west-2'
                           IGNOREHEADER 1
                           format as csv
                          ;
                           """).format(I94_PORT, IAM_ROLE)

i94_mode_table = ("""CREATE TABLE IF NOT EXISTS i94_mode_data (
                    code VARCHAR PRIMARY KEY,
                    mode VARCHAR
                    )
""")

i94_mode_insert = ("""COPY i94_mode_data
                           FROM {}
                           credentials 'aws_iam_role={}'
                           region as 'us-west-2'
                           IGNOREHEADER 1
                           format as csv;
                           """).format(I94_MODE, IAM_ROLE)

i94_addr_table = ("""CREATE TABLE IF NOT EXISTS i94_addr_data(
                    code VARCHAR PRIMARY KEY,
                    addr VARCHAR
                    )
""")

i94_addr_insert = ("""COPY i94_addr_data
                           FROM {}
                           credentials 'aws_iam_role={}'
                           region as 'us-west-2'
                           IGNOREHEADER 1
                           format as csv;
                           """).format(I94_ADDR, IAM_ROLE)

i94_visa_table = ("""CREATE TABLE IF NOT EXISTS i94_visa_data (
                    code VARCHAR PRIMARY KEY,
                    type VARCHAR
                    )
""")

i94_visa_insert = ("""COPY i94_visa_data
                           FROM {}
                           credentials 'aws_iam_role={}'
                          region as 'us-west-2'
                          IGNOREHEADER 1
                           format as csv;
                           """).format(I94_VISA, IAM_ROLE)



#Confirm rows were inserted into each table

us_demographic_load = ("""
select COUNT(*) from us_demographic_data
""")

i94_immigration_load = ("""
select COUNT(*) from i94_immigration_data 
""")

airport_load = ("""
SELECT COUNT(*) from airport_data 
""")

global_temp_load = ("""
SELECT COUNT(*) from global_temp_data 
""")

i94_cntyl_load = ("""
SELECT COUNT(*) from i94_cntyl_data 
""")

i94_port_load  = ("""
SELECT COUNT(*) from i94_port_data 
""")

i94_mode_load  = ("""
Select count(*) from i94_mode_data 
""")

i94_addr_load = ("""
Select count(*) from i94_addr_data
""")

i94_visa_load = ("""
Select count(*) from i94_visa_data 
""")



# QUERY LISTS
create_table_queries = [i94_immigration_table, us_demographic_table, airport_table, global_temp_table, i94_cntyl_table, i94_port_table, i94_mode_table, i94_addr_table, i94_visa_table]

drop_table_queries = [us_demographic_data_drop, i94_immigration_drop, airport_data_drop, global_temp_data_drop, i94_cntyl_data_drop, i94_port_data_drop, i94_mode_data_drop, i94_addr_data_drop, i94_visa_data_drop]

insert_table_queries = [airport_table_insert, us_demographic_table_insert, i94_immigration_insert,  global_temp_insert, i94_cntyl_insert, i94_port_insert, i94_mode_insert, i94_visa_insert, i94_addr_insert]

#Check table size
table_loads = [us_demographic_load, i94_immigration_load, airport_load, global_temp_load, i94_cntyl_load, i94_port_load,i94_mode_load, i94_addr_load, i94_visa_load]
