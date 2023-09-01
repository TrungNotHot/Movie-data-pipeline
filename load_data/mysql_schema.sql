CREATE DATABASE IF NOT EXISTS movies_db;
USE movies_db;
DROP TABLE IF EXISTS metadata;
CREATE TABLE metadata (
  title VARCHAR(2000),
  directedBy VARCHAR(2000),
  starring VARCHAR(2000),
  avgRating FLOAT,
  imdbId INT(15),
  item_id INT(15) NOT NULL,
  PRIMARY KEY (item_id)
);

CREATE TABLE IF NOT EXISTS survey_answers (
  user_id INT(15) NOT NULL,
  item_id INT(15) NOT NULL,
  tag_id INT(15) NOT NULL,
  score FLOAT,
  PRIMARY KEY (user_id, item_id, tag_id)
);

CREATE TABLE IF NOT EXISTS tag_count (
  item_id INT(15),
  tag_id INT(15),
  num INT(15),
  PRIMARY KEY (item_id, tag_id)
);

CREATE TABLE IF NOT EXISTS tags (
  tag VARCHAR(50),
  id INT(15),
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS reviews (
  item_id INT(15),
  txt VARCHAR(2000),
  PRIMARY KEY (item_id)
);

CREATE TABLE IF NOT EXISTS ratings(
  item_id INT(15),
  user_id INT(15),
  rating FLOAT,
  PRIMARY KEY (item_id, user_id)
);
 