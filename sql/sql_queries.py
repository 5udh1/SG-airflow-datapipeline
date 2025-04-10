import configparser
import psycopg2

# CONFIGURATION
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
logdata_table_drop = "DROP TABLE IF EXISTS logdata;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
logdata_table_create = ("""
CREATE TABLE logdata (
    artist VARCHAR(450),
    auth VARCHAR(450),
    firstname VARCHAR(450),
    gender VARCHAR(50),
    iteminsession INTEGER,
    lastname VARCHAR(450),
    length DOUBLE PRECISION,
    level VARCHAR(450),
    location VARCHAR(450),
    method VARCHAR(450),
    page VARCHAR(450),
    registration VARCHAR(450),
    sessionid INTEGER,
    song VARCHAR(65535),
    status INTEGER,
    tstamp BIGINT,
    useragent VARCHAR(450),
    userid INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs INTEGER NOT NULL,
    artist_id VARCHAR(256) NOT NULL,
    artist_latitude NUMERIC(9, 6),
    artist_longitude NUMERIC(9, 6),
    artist_location VARCHAR(256),
    artist_name VARCHAR(256) NOT NULL,
    song_id VARCHAR(256) NOT NULL,
    title VARCHAR(256) NOT NULL,
    duration NUMERIC(18, 0),
    year NUMERIC(4, 0)
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    level VARCHAR(256) NOT NULL,
    song_id VARCHAR(256) NOT NULL,
    artist_id VARCHAR(256) NOT NULL,
    session_id VARCHAR(256) NOT NULL,
    location VARCHAR(256) NOT NULL,
    user_agent VARCHAR(256) NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id INTEGER NOT NULL PRIMARY KEY,
    first_name VARCHAR(256) NOT NULL,
    last_name VARCHAR(256) NOT NULL,
    gender VARCHAR(256) NOT NULL,
    level VARCHAR(256) NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR(256) NOT NULL PRIMARY KEY,
    title VARCHAR(256) NOT NULL,
    artist_id VARCHAR(256) NOT NULL,
    year INTEGER NOT NULL,
    duration NUMERIC(10, 2) NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artists_id VARCHAR(256) NOT NULL PRIMARY KEY,
    name VARCHAR(256),
    location VARCHAR(256),
    latitude NUMERIC(9, 6),
    longitude NUMERIC(9, 6)
);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY,
    hour NUMERIC(2, 0) NOT NULL,
    day NUMERIC(2, 0) NOT NULL,
    week NUMERIC(2, 0) NOT NULL,
    month NUMERIC(2, 0) NOT NULL,
    year NUMERIC(4, 0) NOT NULL,
    weekday NUMERIC(1, 0) NOT NULL
);
""")

# COPY DATA FROM S3 BUCKET
iam_role = config.get('DWH', 'DWH_IAM_ROLE_NAME')
region = config.get('DWH', 'DWH_REGION')  # Ensure 'region' is defined in dwh.cfg

logdata_copy = f"""
COPY logdata
FROM 's3://udacity-dend/log_data'
IAM_ROLE '{iam_role}'
FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
REGION AS '{region}';
"""

songs_copy = f"""
COPY staging_songs (num_songs, artist_id, artist_latitude, artist_longitude, artist_location,
                    artist_name, song_id, title, duration, year)
FROM 's3://udacity-dend/song_data'
IAM_ROLE '{iam_role}'
FORMAT AS JSON 'auto'
REGION AS '{region}';
"""

# INSERT QUERIES
songplays_insert = ("""
INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
)
SELECT DISTINCT 
    TIMESTAMP 'epoch' + t1.tstamp/1000 * interval '1 second' AS start_time,
    t1.userid,
    t1.level,
    t2.song_id,
    t2.artist_id,
    t1.sessionid,
    t1.location,
    t1.useragent
FROM logdata t1
JOIN staging_songs t2  
ON t2.artist_name = t1.artist
AND t1.page = 'NextSong'
AND t1.song = t2.title;
""")

songs_insert = ("""
INSERT INTO songs (
    song_id, title, artist_id, year, duration
) 
SELECT DISTINCT 
    t1.song_id, t1.title, t1.artist_id, t1.year, t1.duration
FROM staging_songs t1;
""")

users_insert = ("""
INSERT INTO users (
    user_id, first_name, last_name, gender, level
) 
SELECT DISTINCT 
    t3.userid, t3.firstname, t3.lastname, t3.gender, t3.level
FROM (
    SELECT t1.userid, MAX(t1.tstamp) AS max_tstamp
    FROM logdata t1
    WHERE t1.page = 'NextSong'
    GROUP BY t1.userid
) t2
JOIN logdata t3 
ON t3.userid = t2.userid
AND t3.tstamp = t2.max_tstamp
AND t3.page = 'NextSong';
""")

artists_insert = ("""
INSERT INTO artists (
    artists_id, name, location, latitude, longitude
) 
SELECT DISTINCT 
    t1.artist_id, t1.artist_name, t1.artist_location, t1.artist_latitude, t1.artist_longitude
FROM staging_songs t1;
""")

time_insert = ("""
INSERT INTO time 
SELECT DISTINCT 
    TIMESTAMP 'epoch' + tstamp/1000 * interval '1 second' AS start_time,
    EXTRACT(hour FROM start_time),
    EXTRACT(day FROM start_time),
    EXTRACT(week FROM start_time),
    EXTRACT(month FROM start_time),
    EXTRACT(year FROM start_time),
    EXTRACT(weekday FROM start_time)
FROM logdata
WHERE tstamp IS NOT NULL;
""")

# QUERY LISTS
create_table_queries = [
    logdata_table_create, 
    staging_songs_table_create, 
    songplay_table_create, 
    user_table_create, 
    song_table_create, 
    artist_table_create, 
    time_table_create
]

drop_table_queries = [
    logdata_table_drop, 
    staging_songs_table_drop, 
    songplay_table_drop, 
    user_table_drop, 
    song_table_drop, 
    artist_table_drop, 
    time_table_drop
]

copy_table_queries = [logdata_copy, songs_copy]
insert_table_queries = [songplays_insert, songs_insert, users_insert, artists_insert, time_insert]