SELECT c.id ,
		c."cast" ,
		c.crew ,
		k.keywords 
FROM
	{{source('movies_db', 'credits')}} c,
	{{source('movies_db', 'keywords')}} k
WHERE c.id = k.id
ORDER BY id

