import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create = ("""
    create table if not exists staging_events
    (
        event_id bigint identity(0,1),
        artist varchar(max),
        auth varchar(max),
        first_name varchar(max),
        gender char,
        item_in_session int,
        last_name varchar(max),
        length float,
        level varchar(max),
        location varchar(max),
        method varchar(max),
        page varchar(max),
        registration numeric,
        session_id int,
        song varchar,
        status varchar,
        ts bigint,
        user_agent varchar(max),
        user_id int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        song_id varchar(max) not null,
        title varchar(max),
        duration numeric,
        year int,
        artist_id varchar(max) not null,
        artist_name varchar(max),
        artist_latitude double precision,
        artist_longitude double precision,
        artist_location varchar(max),
        num_songs int
    )
""")

songplay_table_create = ("""
    create table if not exists songplays(
        songplay_id int identity(0,1) primary key,
        start_time bigint,
        user_id int,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int,
        location varchar,
        user_agent varchar
    )
""")

user_table_create = ("""
    create table if not exists users(
        user_id int not null primary key,
        first_name varchar,
        last_name varchar,
        gender char,
        level varchar
    )
""")

song_table_create = ("""
    create table if not exists songs(
        song_id varchar not null primary key,
        title varchar,
        artist_id varchar,
        year int,
        duration numeric
    )
""")

artist_table_create = ("""
    create table if not exists artists(
        artist_id varchar not null primary key,
        name varchar,
        location varchar,
        latitude double precision,
        longitude double precision
    )
""")

time_table_create = ("""
    create table if not exists time(
        start_time timestamp not null primary key,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    format as json 's3://udacity-dend/log_json_path.json'
    region 'us-west-2'
""").format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
    copy staging_songs
    from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2'
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT se.ts, se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
    FROM staging_events se
    JOIN staging_songs ss
    ON se.artist = ss.artist_name
    where se.page = 'NextSong' AND se.user_id is not null
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(user_id), first_name, last_name, gender, level
    FROM staging_events
    WHERE user_id is not null
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id), title, artist_id, year, duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id), artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
    Select distinct start_time
    ,EXTRACT(HOUR FROM start_time) As hour
    ,EXTRACT(DAY FROM start_time) As day
    ,EXTRACT(WEEK FROM start_time) As week
    ,EXTRACT(MONTH FROM start_time) As month
    ,EXTRACT(YEAR FROM start_time) As year
    ,EXTRACT(DOW FROM start_time) As weekday
    FROM (
    SELECT distinct ts,'1970-01-01'::date + ts/1000 * interval '1 second' as start_time
    FROM staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
