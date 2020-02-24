import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = ("DROP TABLE IF EXISTS staging_events")
staging_songs_table_drop = ("DROP TABLE IF EXISTS staging_songs")
songplay_table_drop = ("DROP TABLE IF EXISTS songplays")
user_table_drop = ("DROP TABLE IF EXISTS users")
song_table_drop = ("DROP TABLE IF EXISTS songs")
artist_table_drop = ("DROP TABLE IF EXISTS artists")
time_table_drop = ("DROP TABLE IF EXISTS time")

# CREATE TABLES

"""   
    Staging table to store the app events as captured in the log files stored on S3. 
    Once the events are loaded in the staging table, they are transformed and loaded into 
    other tables in the star style schema database optimized for analytics queries.

    By setting table Backup to "NO" Snapshot will not backup the table as it is not needed.
    Distribution style "ALL" replicates the table in each node.
"""
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        event_id BIGINT IDENTITY(0,1),
        artist_name VARCHAR(256),
        auth VARCHAR(32),
        user_first_name VARCHAR(256),
        user_gender  VARCHAR(1),
        item_in_session INTEGER,
        user_last_name VARCHAR(256),
        song_length REAL,
        user_level VARCHAR(32),
        location VARCHAR(256),
        method VARCHAR(16),
        page VARCHAR(32),
        registration VARCHAR(32),
        session_id BIGINT,
        song_title VARCHAR(256),
        status INTEGER,
        ts BIGINT,
        user_agent TEXT,
        user_id VARCHAR(128))
    BACKUP NO
    DISTSTYLE ALL;
""")

"""
    Staging table to store the songs from the Million Song Dataset as provided in individual JSON files on S3.
    This staging table is then used to load other tables with transformation of the songs data
    and loaded into other tables in the star style schema database optimized for analytics queries.

    By setting table BACKUP to "NO" Snapshot will not backup the table as it is not needed.
    Distribution style "ALL" replicates the table in each node.
"""
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        song_id VARCHAR(128),
        num_songs INTEGER,
        artist_id VARCHAR(128),
        artist_latitude REAL,
        artist_longitude REAL,
        artist_location VARCHAR(256),
        artist_name VARCHAR(256),
        title VARCHAR(256),
        duration DOUBLE PRECISION,
        year INTEGER)
    BACKUP NO
    DISTSTYLE ALL;
""")

"""
    Facts table in the star style database schema.
    
    Using song_id as a DISTKEY key to enable colocating the songs table in the same node with this table.
"""
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id BIGINT IDENTITY(0, 1) PRIMARY KEY,
        start_time TIMESTAMP REFERENCES time(start_time),
        user_id VARCHAR(128) REFERENCES users(user_id),
        level VARCHAR(32),
        song_id VARCHAR(128) SORTKEY DISTKEY REFERENCES songs(song_id),
        artist_id VARCHAR(128) REFERENCES artists(artist_id),
        session_id BIGINT,
        location VARCHAR(256),
        user_agent TEXT)
    DISTSTYLE KEY;
""")

""" 
    Users dimension table.
"""
user_table_create = ("""
    CREATE TABLE users(
        user_id VARCHAR(128) SORTKEY PRIMARY KEY,
        first_name VARCHAR(128),
        last_name VARCHAR(128),
        gender VARCHAR(1),
        level VARCHAR(32))
    DISTSTYLE ALL;
""")

""" 
    Songs dimension table.

    Using song_id as a DISTKEY key to enable colocating the Facts table in the same node with this table.
"""
song_table_create = ("""
    CREATE TABLE songs(
        song_id VARCHAR(128) SORTKEY DISTKEY PRIMARY KEY,
        title VARCHAR(256),
        artist_id VARCHAR(128) NOT NULL,
        year INTEGER,
        duration REAL)
    DISTSTYLE KEY;
""")

""" 
    Artists dimension table
"""
artist_table_create = ("""
    CREATE TABLE artists(
        artist_id VARCHAR(128) SORTKEY PRIMARY KEY,
        name VARCHAR(256) NOT NULL,
        location VARCHAR(256),
        latitude DECIMAL(8,5),
        longitude DECIMAL(8,5))
    DISTSTYLE ALL;
""")

""" 
    Time dimension table.
"""
time_table_create = ("""
    CREATE TABLE time(
        start_time TIMESTAMP SORTKEY PRIMARY KEY,
        hour INTEGER NOT NULL,         
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday INTEGER NOT NULL)
    DISTSTYLE ALL;
""")


# STAGING TABLES

# Populate events staging table loading from S3 using LOG_JSONPATH configuration variable
staging_events_copy = ("""COPY staging_events FROM '{}'
                          CREDENTIALS 'aws_iam_role={}'
                          FORMAT AS JSON '{}'
                          region 'us-west-2';
                       """).format(config.get('S3','LOG_DATA'),
                                   config.get('IAM_ROLE', 'ARN'),
                                   config.get('S3','LOG_JSONPATH'))

# Populate songs staging table loading from S3 using JSON Array
staging_songs_copy = ("""COPY staging_songs FROM '{}'
                         CREDENTIALS 'aws_iam_role={}'
                         FORMAT AS JSON 'auto'
                         region 'us-west-2' maxerror as 100;
                      """).format(config.get('S3','SONG_DATA'), 
                                  config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES


user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        user_id AS user_id,
        user_first_name AS first_name,
        user_last_name AS last_name,
        user_gender AS gender,
        user_level AS level
    FROM staging_events;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT 
        song_id AS song_id,
        title AS title,
        artist_id AS artist_id,
        year AS year,
        duration AS duration
FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
        artist_id AS artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
FROM staging_songs;
""")

"""
    Note: "Dateparts for Date or Time Stamp Functions and for use with EXTRACT":
        https://docs.aws.amazon.com/redshift/latest/dg/r_Dateparts_for_datetime_functions.html
"""
time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
        start_time, 
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(dw from start_time) AS weekday 
    FROM (
        SELECT DISTINCT (TIMESTAMP 'epoch' + (ts / 1000 ) * INTERVAL '1 second') as start_time 
        FROM staging_events     
    )
""")

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        TIMESTAMP 'epoch' + events.ts/1000 * interval '1 second' AS start_time,
        events.user_id AS user_id,
        events.user_level AS level,
        songs.song_id AS song_id,
        songs.artist_id AS artist_id,
        events.session_id AS session_id,
        songs.artist_location AS location,
        events.user_agent AS user_agent
    FROM staging_events events
    JOIN staging_songs songs
        ON (events.artist_name = songs.artist_name 
            AND events.song_title = songs.title)
    WHERE events.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy ,staging_events_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
