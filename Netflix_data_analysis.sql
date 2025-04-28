USE movies_tvshows_db;

SELECT * FROM netflix_raw ORDER BY show_id;

-- handle foregin characters. Create Table to describe datatypes and append table from python
DROP TABLE netflix_raw;

CREATE TABLE netflix_raw (
	show_id VARCHAR(10) PRIMARY KEY,
	type VARCHAR(10),
	title VARCHAR(200),
	director VARCHAR(300),
	cast VARCHAR(1000),
	country VARCHAR(200),
	date_added VARCHAR(20),
	release_year INT,
	rating VARCHAR(10),
	duration VARCHAR(20),
	listed_in VARCHAR(200),
	description VARCHAR(500)
);

DESCRIBE netflix_raw;

SELECT * FROM netflix_raw ORDER BY title;

-- remove duplicates and set show_id as primary key
SELECT show_id, COUNT(*)
FROM netflix_raw
GROUP BY show_id
HAVING COUNT(*) > 1;

-- check if titles have any duplicate - also check type as same name can be for movie and tv-show
SELECT title, type
FROM netflix_raw
GROUP BY title, type
HAVING COUNT(*) > 1;

SELECT * FROM netflix_raw
WHERE CONCAT(title, type) IN (
SELECT CONCAT(title, type)
FROM netflix_raw
GROUP BY title, type
HAVING COUNT(*) > 1
)
ORDER BY title;

WITH cte AS (
SELECT *, RANK() OVER(PARTITION BY title, type ORDER BY show_id) AS rn
FROM netflix_raw
)
SELECT *
FROM cte
WHERE rn=1;

-- New table for listed in, director, country, cast
-- example for director
-- for SQL server - SELECT show_id, LTRIM(RTRIM(VALUE)) AS director INTO netflix_directors FROM netflix_raw CROSS APPLY STRING_SPLIT(director, ',');
SELECT 
    MAX(
        CHAR_LENGTH(director) - CHAR_LENGTH(REPLACE(director, ',', '')) + 1
    ) AS max_number_of_directors
FROM 
    netflix_raw
WHERE 
    director IS NOT NULL;

CREATE TABLE netflix_directors AS
SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(director, ',', numbers.n), ',', -1)) AS director
FROM 
    netflix_raw
JOIN (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13
) numbers
ON CHAR_LENGTH(director) - CHAR_LENGTH(REPLACE(director, ',', '')) >= numbers.n - 1
WHERE director IS NOT NULL;

-- country
SELECT 
    MAX(
        CHAR_LENGTH(country) - CHAR_LENGTH(REPLACE(country, ',', '')) + 1
    ) AS max_number_of_countries
FROM 
    netflix_raw
WHERE 
    country IS NOT NULL;

CREATE TABLE netflix_countries AS
SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(country, ',', numbers.n), ',', -1)) AS country
FROM 
    netflix_raw
JOIN (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    UNION ALL SELECT 11 UNION ALL SELECT 12
) numbers
ON CHAR_LENGTH(country) - CHAR_LENGTH(REPLACE(country, ',', '')) >= numbers.n - 1
WHERE country IS NOT NULL;

-- cast
SELECT 
    MAX(
        CHAR_LENGTH(cast) - CHAR_LENGTH(REPLACE(cast, ',', '')) + 1
    ) AS max_number_of_casts
FROM 
    netflix_raw
WHERE 
    cast IS NOT NULL;

CREATE TABLE netflix_casts AS
SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(cast, ',', numbers.n), ',', -1)) AS cast
FROM 
    netflix_raw
JOIN (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
    UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20
    UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23 UNION ALL SELECT 24 UNION ALL SELECT 25
    UNION ALL SELECT 26 UNION ALL SELECT 27 UNION ALL SELECT 28 UNION ALL SELECT 29 UNION ALL SELECT 30
    UNION ALL SELECT 31 UNION ALL SELECT 32 UNION ALL SELECT 33 UNION ALL SELECT 34 UNION ALL SELECT 35
    UNION ALL SELECT 36 UNION ALL SELECT 37 UNION ALL SELECT 38 UNION ALL SELECT 39 UNION ALL SELECT 40
    UNION ALL SELECT 41 UNION ALL SELECT 42 UNION ALL SELECT 43 UNION ALL SELECT 44 UNION ALL SELECT 45
    UNION ALL SELECT 46 UNION ALL SELECT 47 UNION ALL SELECT 48 UNION ALL SELECT 49 UNION ALL SELECT 50    
) numbers
ON CHAR_LENGTH(cast) - CHAR_LENGTH(REPLACE(cast, ',', '')) >= numbers.n - 1
WHERE cast IS NOT NULL;

-- listed_in - genre
SELECT 
    MAX(
        CHAR_LENGTH(listed_in) - CHAR_LENGTH(REPLACE(listed_in, ',', '')) + 1
    ) AS max_number_of_listed_in
FROM 
    netflix_raw
WHERE 
    listed_in IS NOT NULL;

CREATE TABLE netflix_genre AS
SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(listed_in, ',', numbers.n), ',', -1)) AS genre
FROM 
    netflix_raw
JOIN (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
) numbers
ON CHAR_LENGTH(listed_in) - CHAR_LENGTH(REPLACE(listed_in, ',', '')) >= numbers.n - 1
WHERE listed_in IS NOT NULL;

SHOW TABLES;

-- data type conversions for date added
-- populate missing values in country, duration columns
-- countries
INSERT INTO netflix_countries
SELECT 
	show_id, m.country
FROM 
	netflix_raw nr
INNER JOIN (
SELECT 
	nd.director, nc.country
FROM 
	netflix_countries nc
INNER JOIN 
	netflix_directors nd 
ON 
	nc.show_id = nd.show_id
GROUP BY nd.director, nc.country
) m ON FIND_IN_SET(m.director, nr.director) > 0
WHERE nr.country IS NULL
ORDER BY nr.country;

SELECT * FROM netflix_raw WHERE director = 'Ahishor Solomon';

SELECT nd.director, nc.country
FROM netflix_countries nc
INNER JOIN netflix_directors nd 
ON nc.show_id = nd.show_id
GROUP BY nd.director, nc.country;

-- duration
SELECT *
FROM netflix_raw
WHERE duration IS NULL;

-- populate rest of null as not available
-- drop columns director, listed_in, country, cast

CREATE TABLE netflix AS
SELECT 
    show_id,
    type, 
    title, 
    STR_TO_DATE(date_added, '%M %d, %Y') AS date_added, 
    release_year, 
    rating, 
    (CASE WHEN duration IS NULL THEN rating ELSE duration END) AS duration, 
    description
FROM (
    SELECT *, 
           RANK() OVER(PARTITION BY title, type ORDER BY show_id) AS rn
    FROM netflix_raw
) cte
WHERE rn = 1;

-- NETFLIX DATA ANALYSIS

-- 1) For each director count the number of movies and tv-shows created by them in seperate columns for directors who has created both movies and tv-shows
SELECT n.show_id, n.type, nd.director
FROM netflix n
INNER JOIN netflix_directors nd
ON n.show_id = nd.show_id;

SELECT nd.director, COUNT(DISTINCT n.type) AS num_type
FROM netflix n
INNER JOIN netflix_directors nd
ON n.show_id = nd.show_id
GROUP BY nd.director
HAVING num_type > 1;

SELECT nd.director, 
COUNT(DISTINCT CASE WHEN n.type = 'Movie' THEN n.show_id END) AS no_of_movies,
COUNT(DISTINCT CASE WHEN n.type = 'TV Show' THEN n.show_id END) AS no_of_tv_shows
FROM netflix n
INNER JOIN netflix_directors nd
ON n.show_id = nd.show_id
GROUP BY nd.director
HAVING COUNT(DISTINCT n.type) > 1;

-- 2) which country has highest number of comedy movies
SELECT ng.show_id, nc.country 
FROM netflix_genre ng
INNER JOIN netflix_countries nc ON ng.show_id = nc.show_id
WHERE ng.genre = 'Comedies';

SELECT nc.country, COUNT(DISTINCT ng.show_id) AS no_of_comedy_movies 
FROM netflix_genre ng
INNER JOIN netflix_countries nc ON ng.show_id = nc.show_id
INNER JOIN netflix n ON ng.show_id = n.show_id
WHERE ng.genre = 'Comedies' AND n.type = 'Movie'
GROUP BY nc.country
ORDER BY no_of_comedy_movies DESC
LIMIT 1;

-- 3) for each year (as per date added to netflix), which director has maximum number of movies released
SELECT nd.director, YEAR(n.date_added) AS date_year, COUNT(n.show_id) AS no_of_movies
FROM netflix n
INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
WHERE n.type = 'Movie'
GROUP BY date_year, director
ORDER BY no_of_movies DESC;

WITH cte AS (
SELECT nd.director, YEAR(n.date_added) AS date_year, COUNT(n.show_id) AS no_of_movies
FROM netflix n
INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
WHERE n.type = 'Movie'
GROUP BY director, date_year
),
cte2 AS (
SELECT *, RANK() OVER(PARTITION BY date_year ORDER BY no_of_movies DESC, director) AS rn
FROM cte)
SELECT * FROM cte2
WHERE rn = 1;

-- 4) What is average duration of movies in each genre
SELECT ng.genre, ROUND(AVG(CAST(REPLACE(n.duration, ' min', '') AS UNSIGNED)), 2) AS avg_duration
FROM netflix n
INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
WHERE n.type = 'Movie'
GROUP BY ng.genre
ORDER BY avg_duration DESC;

-- 5) find the list of directors who have created genre horror and comedy both - display the director name along with number of comedy and horror directed by them
SELECT n.show_id, nd.director, ng.genre
FROM netflix n
INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
WHERE n.type = 'Movie';


WITH cte AS (
SELECT n.show_id, nd.director, ng.genre
FROM netflix n
INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
WHERE n.type = 'Movie'
)
SELECT director,
COUNT(CASE WHEN genre = 'Comedies' THEN 1 END) AS no_of_comedies,
COUNT(CASE WHEN genre = 'Horror Movies' THEN 1 END) AS no_of_horrors
FROM cte
GROUP BY director
HAVING no_of_comedies >= 1 AND no_of_horrors >= 1;
