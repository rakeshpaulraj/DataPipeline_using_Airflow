class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        select  
              cast(userId as integer) as userId
            , firstName
            , lastName
            , gender
            , max(level) as level
        from staging_events
        where nvl(trim(userid), '') <> ''
        and page='NextSong'
        group by userId, firstName, lastName, gender
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    user_table_duplicate_check = ("""
        SELECT count(*) from (select userid from users group by userid having count(*) > 1) a
    """)  
    
    user_table_userid_null_check = ("""
        SELECT count(*) from users where userid is null
    """) 
	
	user_table_userid_numeric_check = ("""
        SELECT count(*) from users where lower(userid) <> upper(userid)
    """) 
    