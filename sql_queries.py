import configparser


##################### 
# CONFIG
#####################

config = configparser.ConfigParser()
config.read('dwh.cfg')


##################### 
# DROP TABLES
#####################

staging_events_table_drop = "DROP table IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs_table"
songplay_table_drop = "DROP table IF EXISTS songplay_table"
user_table_drop = "DROP table IF EXISTS user_table"
song_table_drop = "DROP table IF EXISTS song_table"
artist_table_drop = "DROP table IF EXISTS artist_table"
time_table_drop = "DROP table IF EXISTS time_table"


##################### 
# CREATE TABLES
#####################

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table (
    event_id        INT IDENTITY(0, 1)  NOT NULL    SORTKEY DISTKEY,
    artist          VARCHAR,
    auth            VARCHAR,
    firstname      VARCHAR,
    gender          VARCHAR,
    itemInSession   INTEGER,
    lastname       VARCHAR,
    length          FLOAT,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR(4),
    page            VARCHAR,
    registrtion     BIGINT,
    sessionId       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              TIMESTAMP           NOT NULL,
    userAgent       VARCHAR,
    userId          INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table (
    num_songs       INTEGER    NOT NULL    SORTKEY DISTKEY,
    artist_id       VARCHAR    NOT NULL,
    latitude        DECIMAL,
    longitude       DECIMAL,
    location        VARCHAR,
    artist_name     VARCHAR    NOT NULL,
    song_id         VARCHAR    NOT NULL,
    title           VARCHAR    NOT NULL,
    duration        DECIMAL    NOT NULL,
    year            INTEGER    NOT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table (
    songplay_id     INTEGER     IDENTITY(0,1)   PRIMARY KEY SORTKEY,
    start_time      TIMESTAMP   NOT NULL,
    user_id         INTEGER     NOT NULL,
    level           VARCHAR     NOT NULL,
    song_id         VARCHAR     NOT NULL,
    artist_id       VARCHAR     NOT NULL,
    session_id      INTEGER     NOT NULL,
    location        VARCHAR     NOT NULL,
    user_agent      VARCHAR     NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users_table (
    user_id         INTEGER     NOT NULL    PRIMARY KEY DISTKEY,
    first_name      VARCHAR     NOT NULL,
    last_name       VARCHAR     NOT NULL,
    gender          CHAR(1)     NOT NULL,
    level           VARCHAR     NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table (
    song_id         VARCHAR     NOT NULL    PRIMARY KEY,
    title           VARCHAR     NOT NULL,
    artist_id       VARCHAR     NOT NULL    DISTKEY,
    year            INTEGER     NOT NULL,
    duration        DECIMAL     NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table (
    artist_id       VARCHAR     NOT NULL    PRIMARY KEY DISTKEY,
    name            VARCHAR     NOT NULL,
    location        VARCHAR     NOT NULL,
    latitude        DECIMAL     NOT NULL,
    longitude       DECIMAL     NOT NULL
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table (
    start_time      TIMESTAMP       NOT NULL    PRIMARY KEY SORTKEY DISTKEY,
    hour            INTEGER         NOT NULL,
    day             INTEGER         NOT NULL,
    week            INTEGER         NOT NULL,
    month           INTEGER         NOT NULL,
    year            INTEGER         NOT NULL,
    weekday         INTEGER         NOT NULL
);
""")


##################### 
# STAGING TABLES
#####################

##### Config Variables #####
cfg_log_data = config['S3'].get('LOG_DATA')
cfg_iam = config['IAM_ROLE'].get('ARN').strip("'")
cfg_logpath = config['S3'].get('LOG_JSONPATH')
cfg_songdata = config['S3'].get('SONG_DATA')
##### Config Variables #####

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role = {}'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    COMPUPDATE OFF REGION 'us-west-2'
    FORMAT AS JSON {};
    """).format(cfg_log_data,
                cfg_iam,
                cfg_logpath)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role = {}'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON 'auto'
    COMPUPDATE OFF REGION 'us-west-2';
    """).format(cfg_songdata,
                cfg_iam)


##################### 
# INSERT DATA IN TABLES
#####################

songplay_table_insert = ("""
INSERT INTO songplay_table(start_time,
                           user_id,
                           level,
                           song_id,
                           artist_id,
                           session_id,
                           location,
                           user_agent)
SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),
                             'YYYY-MM-DD HH24:MI:SS'),
                se.userId AS user_id,
                se.level AS level,
                ss.song_id AS song_id,
                ss.artist_id AS artist_id,
                se.sessionId AS session_id,
                se.location AS location,
                se.userAgent AS user_agent
FROM staging_events_table se
JOIN staging_songs_table ss ON se.song = ss.title 
                      AND se.artist = ss.artist_name
                      AND se.length = ss.duration;
""")

user_table_insert = ("""
INSERT INTO user_table(user_id,
                       first_name,
                       last_name,
                       gender,
                       level)
SELECT DISTINCT userId AS user_id,
                firstName AS first_name,
                lastName AS last_name,
                gender AS gender,
                level AS level
FROM staging_events_table
WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO song_table(song_id,
                       title,
                       artist_id,
                       year,
                       duration)
SELECT DISTINCT song_id AS song_id,
                title AS title,
                artist_id AS artist_id,
                year AS year,
                duration AS duration
FROM staging_songs_table
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artist_table(artist_id,
                         name,
                         location,
                         latitude,
                         longitude)
SELECT DISTINCT artist_id AS artist_id,
                artist_name AS name,
                location AS location,
                latitude AS latitude,
                longitude AS longitude
FROM staging_songs_table
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time_table(start_time,
                       hour,
                       day,
                       week,
                       month,
                       year,
                       weekday)
SELECT DISTINCT ts,
                EXTRACT(hour FROM ts),
                EXTRACT(day FROM ts),
                EXTRACT(week FROM ts),
                EXTRACT(month FROM ts),
                EXTRACT(year FROM ts),
                EXTRACT(weekday FROM ts)
FROM staging_events_table
WHERE ts IS NOT NULL;
""")


##################### 
# QUERY LISTS
#####################
create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]

drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]

copy_table_queries = [staging_events_copy,
                      staging_songs_copy]

insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]
