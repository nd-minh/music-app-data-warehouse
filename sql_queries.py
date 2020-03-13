import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE staging_events (
        event_id integer IDENTITY(0, 1),
        artist varchar(max),
        auth varchar(30),
        firstName varchar(30),
        gender varchar(20),
        itemInSession integer,
        lastName varchar(30),
        length numeric,
        level varchar(30),
        location varchar(max),
        method varchar(20),
        page varchar(20),
        registration bigint,
        sessionId integer,
        song varchar(max),
        status integer,
        ts bigint NOT NULL, 
        userAgent varchar(max),
        userId varchar(20) NOT NULL,
        PRIMARY KEY(event_id))
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs integer,
        artist_id varchar(30) NOT NULL,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar(max),
        artist_name varchar(max),
        song_id varchar(30) NOT NULL,
        title varchar(max),
        duration numeric,
        year integer,
        PRIMARY KEY(song_id))
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id integer IDENTITY(0,1),
        start_time datetime NOT NULL,
        user_id varchar(20) NOT NULL,
        level varchar(30),
        song_id varchar(30) NOT NULL,
        artist_id varchar(30) NOT NULL,
        session_id integer,
        location varchar(max),
        user_agent varchar(max),
        PRIMARY KEY(songplay_id),
        FOREIGN KEY(start_time) REFERENCES time (start_time),
        FOREIGN KEY(user_id) REFERENCES users (user_id),
        FOREIGN KEY(song_id) REFERENCES songs (song_id),        
        FOREIGN KEY(artist_id) REFERENCES artists (artist_id))
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id varchar(30) NOT NULL,
        first_name varchar(max),
        last_name varchar(max),
        gender varchar(20),
        level varchar(30),  
        PRIMARY KEY(user_id))
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id varchar(30) NOT NULL, 
        title varchar(max) NOT NULL,
        artist_id varchar(30) NOT NULL,
        year integer,
        duration numeric NOT NULL,
        PRIMARY KEY(song_id))
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id varchar(30) NOT NULL,
        name varchar(max),
        location varchar(max),
        lattitude numeric,
        longitude numeric,
        PRIMARY KEY(artist_id))
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time datetime NOT NULL,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday integer,
        PRIMARY KEY(start_time))
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events from {}
    iam_role {}
    json {}
    region 'us-west-2'
""").format(config.get('S3','LOG_DATA'), config.get("IAM_ROLE","ARN"), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs from {}    
    iam_role {}
    format as json 'auto'
    region 'us-west-2'   
""").format(config.get('S3','SONG_DATA'), config.get("IAM_ROLE","ARN"))

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent)        
    SELECT
        timestamp 'epoch' + t1.ts/1000 * interval '1 second' AS start_time,
        t1.userId as user_id,
        t1.level,
        t2.song_id,
        t2.artist_id,
        t1.sessionId as session_id,
        t1.location,
        t1.userAgent as user_agent       
    FROM staging_events t1
    JOIN staging_songs t2 ON (t1.artist=t2.artist_name AND t1.length=t2.duration AND t1.song=t2.title)
    WHERE t1.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level)
    SELECT DISTINCT 
        userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level
    FROM
        staging_events  
    
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id, 
        title,
        artist_id,
        year,
        duration)
    SELECT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM
        staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        lattitude,
        longitude
    )
    SELECT
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    FROM 
        staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT
        t.start_time,
        extract(hour from t.start_time),
        extract(day from t.start_time),
        extract(week from t.start_time),
        extract(month from t.start_time),
        extract(year from t.start_time),
        extract(weekday from t.start_time)
    FROM        
        (SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time
        FROM staging_events) AS t
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]