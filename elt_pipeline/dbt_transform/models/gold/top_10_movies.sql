SELECT m.original_title,
	m.title ,
	l.imdbId,
	l.tmdbId,
	m.vote_average,
	m.vote_count,
	m.genres
FROM {{ref('movies')}} m,
     {{ref('links')}} l
WHERE m.id = l.movieId
	AND m.vote_average > 8
ORDER BY (m.vote_count * m.vote_average) DESC
LIMIT 10;