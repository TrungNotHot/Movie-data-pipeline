<<<<<<< HEAD
--create schema movies_db;
--DROP TABLE IF EXISTS movies_db.movies CASCADE;
--CREATE TABLE movies_db.movies (
--  belongs_to_collection TEXT,
--  budget INT4,
--  genres TEXT,
--  id INT4,
--  imdb_id VARCHAR(20),
--  original_language VARCHAR(2),
--  original_title VARCHAR(100),
--  overview TEXT,
--  popularity FLOAT4,
--  poster_path TEXT,
--  production_companies TEXT,
--  release_date DATE,
--  revenue INT4,
--  runtime INT4,
--  spoken_languages TEXT,
--  status VARCHAR(30),
--  tagline TEXT,
--  title VARCHAR(100),
--  vote_average FLOAT4,
--  vote_count INT4,
--  PRIMARY KEY (id)
--);
--
-- DROP TABLE IF EXISTS movies_db.credits CASCADE;
-- CREATE TABLE movies_db.credits (
--   id INT4,
--   "cast" JSON,
--   crew JSON,
--   PRIMARY KEY (id)
-- );
--
-- DROP TABLE IF EXISTS movies_db.keywords CASCADE;
-- CREATE TABLE movies_db.keywords (
--   id INT4,
--   keywords TEXT,
--   PRIMARY KEY (id)
-- );
--
-- DROP TABLE IF EXISTS movies_db.links CASCADE;
-- CREATE TABLE movies_db.links(
--   movieId INT4,
--   imdbId VARCHAR(15),
--   tmdbId VARCHAR(15),
--   PRIMARY KEY (movieId)
-- );
--
-- DROP TABLE IF EXISTS movies_db.ratings CASCADE;
-- CREATE TABLE movies_db.ratings(
--   userId INT4,
--   movieId INT4,
--   rating FLOAT4,
--   timestamp timestamp,
--   PRIMARY KEY (userId, movieId)
-- );
=======
create schema movies_db;
DROP TABLE IF EXISTS movies_db.movies CASCADE;
CREATE TABLE movies_db.movies (
  belongs_to_collection TEXT,
  budget INT4,
  genres TEXT,
  id INT4,
  imdb_id VARCHAR(20),
  original_language VARCHAR(2),
  original_title VARCHAR(100),
  overview TEXT,
  popularity FLOAT4,
  poster_path TEXT,
  production_companies TEXT,
  release_date DATE,
  revenue INT4,
  runtime INT4,
  spoken_languages TEXT,
  status VARCHAR(30),
  tagline TEXT,
  title VARCHAR(100),
  vote_average FLOAT4,
  vote_count INT4,
  PRIMARY KEY (id)
);

 DROP TABLE IF EXISTS movies_db.credits CASCADE;
 CREATE TABLE movies_db.credits (
   id INT4,
   "cast" JSON,
   crew JSON,
   PRIMARY KEY (id)
 );

 DROP TABLE IF EXISTS movies_db.keywords CASCADE;
 CREATE TABLE movies_db.keywords (
   id INT4,
   keywords TEXT,
   PRIMARY KEY (id)
 );

 DROP TABLE IF EXISTS movies_db.links CASCADE;
 CREATE TABLE movies_db.links(
   movieId INT4,
   imdbId VARCHAR(15),
   tmdbId VARCHAR(15),
   PRIMARY KEY (movieId)
 );

 DROP TABLE IF EXISTS movies_db.ratings CASCADE;
 CREATE TABLE movies_db.ratings(
   userId INT4,
   movieId INT4,
   rating FLOAT4,
   timestamp timestamp,
   PRIMARY KEY (userId, movieId)
 );
>>>>>>> 1c675941d7c2863c1b6366af16a5088dedcc625b
