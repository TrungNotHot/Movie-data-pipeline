CREATE DATABASE IF NOT EXISTS movies_db;
USE movies_db;
DROP TABLE IF EXISTS movie;
CREATE TABLE movie (
  belongs_to_collection TEXT,
  budget INT,
  genres TEXT,
  id INT,
  imdb_id VARCHAR(20),
  original_language VARCHAR(2),
  original_title VARCHAR(100),
  overview TEXT,
  popularity FLOAT,
  poster_path TEXT,
  production_companies TEXT,
  release_date DATE,
  revenue INT,
  runtime INT,
  spoken_languages TEXT,
  status VARCHAR(30),
  tagline TEXT,
  title VARCHAR(100),
  vote_average FLOAT,
  vote_count INT,
  PRIMARY KEY (id)
);

 DROP TABLE IF EXISTS credit;
 CREATE TABLE credit (
   id INT(15),
   cast TEXT,
   crew TEXT,
   PRIMARY KEY (id)
 );

 DROP TABLE IF EXISTS keyword;
 CREATE TABLE keyword (
   id INT,
   keywords TEXT,
   PRIMARY KEY (id)
 );

 DROP TABLE IF EXISTS link;
 CREATE TABLE link(
   movieId INT(15),
   imdbId VARCHAR(15),
   tmdbId VARCHAR(15),
   PRIMARY KEY (movieId)
 );

 DROP TABLE IF EXISTS rating;
 CREATE TABLE rating(
   userId INT(15),
   movieId INT(15),
   rating FLOAT,
   timestamp DATETIME,
   PRIMARY KEY (userId, movieId)
 );
