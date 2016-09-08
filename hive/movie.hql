-- Create movie_title table
DROP TABLE IF EXISTS movie_title;
CREATE TABLE movie_title (movie_id int, year_of_release STRING, title STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
-- Load movie_titles.txt to movie_title table
LOAD DATA LOCAL INPATH '${hiveconf:movie_title}' OVERWRITE INTO TABLE movie_title;


-- Create movie_rating table
DROP TABLE IF EXISTS movie_rating;
CREATE TABLE movie_rating (movie_id int, customer_id int, rating DOUBLE, rating_date STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
-- Load generated concat-training-set.txt to table movie_rating
LOAD DATA LOCAL INPATH '${hiveconf:rating_record}' OVERWRITE INTO TABLE movie_rating;


-- table to hold average rating for each movie
DROP TABLE IF EXISTS average_rating;
create table average_rating (movie_id int, avg_rating double);
--calcualte the average rating with at least R nubmer of ratings
from movie_rating INSERT OVERWRITE TABLE average_rating select movie_id, avg(rating) as avg_rating group by movie_id having count(*) > ${hiveconf:R};


-- table to hold records for top M movies
DROP TABLE IF EXISTS top_movies;
create table top_movies (movie_id int, year_of_release String, title String, avg_rating double);
-- Only save top M movie records in this table
FROM average_rating r JOIN movie_title m ON (r.movie_id = m.movie_id) INSERT OVERWRITE TABLE top_movies SELECT m.*, r.avg_rating order by r.avg_rating desc, m.year_of_release desc, m.title limit ${hiveconf:M};


-- table for user who rated lowest for those M movies
DROP TABLE IF EXISTS top_user;
create table top_user (customer_id int, avg_rating double, rating_count int);
-- Select U users who given lowest average ratings for all M movies
FROM top_movies tm JOIN movie_rating mr ON (tm.movie_id = mr.movie_id) INSERT OVERWRITE TABLE top_user SELECT mr.customer_id, avg(mr.rating) as avg_rating, count(*) as rating_count group by mr.customer_id having rating_count = ${hiveconf:M} order by avg_rating, mr.customer_id limit ${hiveconf:U};

-- hold all rating for those U users
DROP TABLE IF EXISTS all_ranking;
create table all_ranking (customer_id int, title String, year_of_release String, rating_date String, rating double);
-- select all ratings for all movies for U users 
FROM top_user tu JOIN movie_rating mr ON (tu.customer_id = mr.customer_id) JOIN movie_title mt ON (mr.movie_id = mt.movie_id) INSERT OVERWRITE TABLE all_ranking SELECT tu.customer_id, mt.title, mt.year_of_release, mr.rating_date, mr.rating; 

-- for final result
DROP TABLE IF EXISTS final_result;
create table final_result (customer_id int, title String, year_of_release String, rating_date String);
-- Generate the final result using Hive builtin function rank()
INSERT OVERWRITE TABLE final_result select a.customer_id, a.title, a.year_of_release, a.rating_date from (SELECT customer_id, title, year_of_release, rating_date, rank() over (PARTITION BY customer_id ORDER BY rating DESC, year_of_release DESC, title) as rank from all_ranking) a where rank = 1;

