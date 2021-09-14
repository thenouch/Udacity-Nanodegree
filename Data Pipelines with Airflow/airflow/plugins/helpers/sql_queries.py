class SqlQueries:
    songplay_table_insert = ("""
        (start_time, 
        user_id,
        level,
        artist_id,
        song_id,
        session_id,
        location,
        user_agent)
        
        SELECT
                events.start_time, 
                events.user_id, 
                events.level,
                songs.artist_id, 
                songs.song_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT distinct ts AS start_time,
                userid AS user_id,
                level,
                artist,
                length,
                sessionId,
                location,
                userAgent,
                song
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
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
        SELECT start_time as start_time, 
        extract(hour from start_time), 
        extract(day from start_time), 
        extract(week from start_time), 
        extract(month from start_time), 
        extract(year from start_time), 
        extract(dayofweek from start_time)
        FROM songplays
    """)