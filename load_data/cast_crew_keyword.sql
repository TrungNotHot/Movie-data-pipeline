SELECT c.id ,
		c."cast" ,
		c.crew ,
		k.keywords 
FROM
	credits c,
	keywords k
WHERE c.id = k.id
order by id

