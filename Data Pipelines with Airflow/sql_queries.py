import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#Set the roles as values
IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events 
(
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INTEGER,
lastName VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration FLOAT,
sessionId INTEGER,
song VARCHAR,
status INTEGER,
ts TIMESTAMP,
userAgent VARCHAR,
userId INTEGER

)
""")
staging_songs_table_create = ("""
CREATE TABLE staging_songs 
(
num_songs INTEGER,
artist_id VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration FLOAT,
year INTEGER

)

""")

songplay_table_create = ("""
CREATE TABLE songplays 
( 
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP NOT NULL SORTKEY DISTKEY,
user_id INTEGER,
level VARCHAR,
artist_id VARCHAR,
song_id VARCHAR,
session_id INTEGER,
location VARCHAR,
user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE users
(
user_id INTEGER NOT NULL SORTKEY PRIMARY KEY,
first_name VARCHAR NOT NULL,
last_name VARCHAR NOT NULL,
gender VARCHAR NOT NULL,
level VARCHAR NOT NULL

)
""")

song_table_create = ("""
CREATE TABLE songs
(
song_id VARCHAR NOT NULL SORTKEY PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR ,
year INTEGER,
duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE artists 
(
artist_id VARCHAR NOT NULL SORTKEY PRIMARY KEY,
name VARCHAR,
location VARCHAR,
latitude FLOAT,
longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE time 
(
start_time TIMESTAMP NOT NULL DISTKEY PRIMARY KEY,
hour INTEGER NOT NULL,
day INTEGER NOT NULL,
week INTEGER NOT NULL,
month INTEGER NOT NULL,
year INTEGER NOT NULL,
weekday VARCHAR(30) NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""

copy staging_events from {}
iam_role {}
format as json {}
timeformat 'epochmillisecs'
region as 'us-west-2'
;

""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)



staging_songs_copy = ("""
copy staging_songs from {}
iam_role {}
format as json 'auto'
STATUPDATE ON
region as 'us-west-2'
;
""").format(SONG_DATA, IAM_ROLE)


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level, artist_id, song_id, session_id, location, user_agent)

SELECT 
DISTINCT se.ts as start_time,
se.userId as user_id,
se.level as level,
ss.artist_id as artist_id,
ss.song_id as song_id,
se.sessionId as sessionId,
se.location as location,
se.userAgent as userAgent
from staging_events se
join staging_songs ss
on se.artist = ss.artist_name
and se.song = ss.title 
and se.length = ss.duration
where se.page ='NextSong';
""")

user_table_insert = ("""
INSERT INTO users
(user_id, first_name, last_name, gender, level)
SELECT 
distinct se.userId as user_id,
se.firstName as first_name,
se.lastName as last_name,
se.gender as gender,
se.level as level
from staging_events se
where se.page ='NextSong';
""")

song_table_insert = ("""
INSERT INTO songs
(song_id, title, artist_id, year, duration)
SELECT 
distinct song_id,
title,
artist_id,
year,
duration
from staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id, name, location, latitude, longitude)

SELECT
distinct artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude
from staging_songs;
""")

time_table_insert = ("""
INSERT INTO time
(start_time, hour, day, week, month, year, weekday)
SELECT
t.start_time,
EXTRACT(hour FROM t.start_time) as hour,
EXTRACT(day FROM t.start_time) as day,
EXTRACT(week FROM t.start_time) as week,
EXTRACT(month FROM t.start_time) as month,
EXTRACT(year FROM t.start_time) as year,
EXTRACT(dayofweek FROM t.start_time) as weekday          
from (select distinct ts as start_time FROM
staging_events where page = 'NextSong') t

;
""")

#Confirm rows were inserted into each table

staging_events_load = ("""
select COUNT(*) from staging_events
""")

staging_songs_load = ("""
select COUNT(*) from staging_songs
""")

songplays_load = ("""
SELECT COUNT(*) from songplays
""")

users_load = ("""
SELECT COUNT(*) from users
""")

songs_load = ("""
SELECT COUNT(*) from songs
""")

artists_load = ("""
SELECT COUNT(*) from artists
""")

time_load = ("""
Select count(*) from time
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

#Check table size
table_loads = [staging_events_load, staging_songs_load, songplays_load , users_load, songs_load, artists_load,time_load ]
