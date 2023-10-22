WITH user_gen AS (
	SELECT r.*, json_array_elements(REPLACE(m.genres, '''', '"')::json)->>'name' AS genres
	FROM {{source('movies_db', 'ratings')}} r, {{source('movies_db', 'movies')}} m
	WHERE r."movieId" = m.id 
	AND r.rating >= 4
)
, ranked_gen AS (
	SELECT
		"userId",
		genres,
		SUM(ug.rating) AS gen_grade,
		ROW_NUMBER() OVER (PARTITION BY "userId" ORDER BY SUM(ug.rating) DESC) AS row_num
	FROM user_gen AS ug
	GROUP BY "userId", ug.genres
)
SELECT "userId", genres, gen_grade
FROM ranked_gen
WHERE row_num <= 5
ORDER BY "userId" ASC, gen_grade DESC
