CREATE DATABASE IF NOT EXISTS movie_db;
USE movie_db;
DROP TABLE IF EXISTS movies_metadata;
CREATE TABLE movies_metadata (
  id INT(10) NOT NULL,
  adult VARCHAR(5) CHECK (adult IN ('True', 'False')),
  belongs_to_collection VARCHAR(255),
  budget INT(11),
  genres VARCHAR(255),
  homepage VARCHAR(255),
  imdb_id VARCHAR(255),
  original_language VARCHAR(5),
  original_title VARCHAR(255),
  overview VARCHAR(10000),
  popularity FLOAT,
  poster_path VARCHAR(255),
  production_companies VARCHAR(1000),
  production_countries VARCHAR(1000),
  release_date DATE,
  revenue INT(11),
  runtime INT(5),
  spoken_languages VARCHAR(1000),
  status VARCHAR(255),
  tagline VARCHAR(255),
  title VARCHAR(255),
  video VARCHAR(5) CHECK (video IN ('True', 'False')),
  vote_average FLOAT,
  vote_count INT(11),
  PRIMARY KEY (id)
);

DROP TABLE IF EXISTS keywords;
CREATE TABLE keywords (
  id INT(10) NOT NULL,
  keywords VARCHAR(10000),
  PRIMARY KEY (id)
);

DROP TABLE IF EXISTS links;
CREATE TABLE links (
  id INT(10) NOT NULL,
  imdb_id INT(10),
  tmdb_id INT(10),
  PRIMARY KEY (id)
);

DROP TABLE IF EXISTS ratings;
CREATE TABLE ratings (
  user_id INT(10) NOT NULL,
  movie_id INT(10) NOT NULL,
  rating FLOAT,
  timestamp INT(20),
  PRIMARY KEY (user_id, movie_id)
);

DROP TABLE IF EXISTS credits;
CREATE TABLE credits (
  id INT(10) NOT NULL,
  cast VARCHAR(10000),
  crew VARCHAR(10000),
  PRIMARY KEY (id)
);
