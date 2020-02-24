# taken directly from my project 3

songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second', se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
FROM staging_events se
JOIN staging_songs ss
ON (se.artist = ss.artist_name AND se.song = ss.title)
WHERE se.page = 'NextSong';
""")
# shouldn't need this anymore: AND se.userId IS NOT NULL;

user_table_insert = ("""
INSERT INTO users
(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE page = 'NextSong';
""")
# shouldn't need this anymore: AND userId IS NOT NULL

song_table_insert = ("""
INSERT INTO songs
(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time
(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT start_time as ts,
EXTRACT(hour from ts),
EXTRACT(day from ts),
EXTRACT(week from ts),
EXTRACT(month from ts),
EXTRACT(year from ts),
EXTRACT(weekday from ts)
FROM songplays;
""")
