#Top Ten most played songs
SELECT s.title, count(sp.songplay_id) play_count
FROM songplays AS sp, songs AS s
WHERE sp.song_id = s.song_id
GROUP BY s.title
ORDER BY play_count DESC
LIMIT 10;

#Top ten locations with most song play count
SELECT sp.location, count(sp.songplay_id) play_count
FROM songplays AS sp
GROUP BY sp.location
ORDER BY play_count DESC
LIMIT 10;

#User counts by Level
SELECT u.level, count(u.user_id) user_count
FROM users AS u
GROUP BY u.level;