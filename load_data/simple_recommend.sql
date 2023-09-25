WITH C AS (
    SELECT AVG(vote_average) AS mean_vote_whole_report
    FROM movies
    WHERE vote_average IS NOT NULL
), m AS (
    SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY vote_count) AS minimum_votes_required
    FROM movies
    WHERE vote_count IS NOT NULL
)
SELECT 
    title,
    EXTRACT(YEAR FROM release_date) AS release_year,
    vote_count,
    vote_average,
    genres,
   	(md.vote_count/(md.vote_count+m.minimum_votes_required)
   	*md.vote_average) +
   	(m.minimum_votes_required/(m.minimum_votes_required
   	+md.vote_count)*C.mean_vote_whole_report) as "wr"
FROM movies as md
CROSS JOIN C
CROSS JOIN m
WHERE vote_count >= m.minimum_votes_required
    AND vote_count IS NOT NULL
    AND vote_average IS NOT null
order by "wr" DESC