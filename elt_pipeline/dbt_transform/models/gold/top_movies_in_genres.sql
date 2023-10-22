WITH movie_genre AS (
    SELECT
        title,
        EXTRACT(YEAR FROM release_date) AS year,
        vote_count,
        vote_average,
        popularity
    FROM
        {{source('movies_db', 'movies')}} m
    where
        genres LIKE '%' || 'Romance' || '%' --change "Romance" to genres you want to
        AND vote_count IS NOT NULL
        AND vote_average IS NOT NULL
), statistics AS (
    SELECT
        AVG(vote_average) AS C,
        percentile_cont(0.85) WITHIN GROUP (ORDER BY vote_count) AS m
    FROM
        movie_genre
)
SELECT
    title,
    year,
    vote_count,
    vote_average,
    popularity,
    ((vote_count / (vote_count + m)) * vote_average) + ((m / (m + vote_count)) * C) AS wr
FROM
    movie_genre
CROSS JOIN
    statistics
WHERE
    vote_count >= m
ORDER BY
    wr DESC
LIMIT 250