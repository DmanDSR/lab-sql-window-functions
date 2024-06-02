USE sakila;

-- Rank films by their length and create an output table that includes the title
-- , length, and rank columns only. Filter out any rows with null or zero values in the length column.

SELECT title, length, RANK() OVER(partition by length ORDER BY title) 'Ranks'
FROM film
WHERE length != null or length != 0;

-- Rank films by length within the rating category and create an output table 
-- that includes the title, length, rating and rank columns only. Filter out any rows 
-- with null or zero values in the length column.

SELECT title, length, rating, RANK() OVER(partition by length ORDER BY rating) as 'ranks'
FROM film
WHERE length != null or length != 0;

-- Produce a list that shows for each film in the Sakila database, the actor 
-- or actress who has acted in the greatest number of films, as well as the total 
-- number of films in which they have acted. Hint: Use temporary tables, CTEs, or
-- Views when appropiate to simplify your queries
/*
CREATE TEMPORARY TABLE sakila.ac_num
WITH cte_1 as (	SELECT film_id, title
				FROM film)
SELECT fa.actor_id, COUNT(fa.film_id) 'Number of films'
FROM film_actor fa
JOIN cte_1 c
ON fa.film_id = c.film_id
WHERE fa.film_id = c.film_id
GROUP BY fa.actor_id
ORDER BY 'Number of films' desc;

SELECT title, film_id, actor_id
from film
*/ 
CREATE TEMPORARY TABLE actor_film_coun
SELECT actor_id, COUNT(*) AS film_count
FROM film_actor
GROUP BY actor_id;

CREATE VIEW film_actor_ranks AS
WITH actor_ranks AS (
  SELECT
    actor_id,
    film_count,
    RANK() OVER (ORDER BY film_count DESC) as actor_rank
  FROM
    actor_film_coun)
SELECT
  f.film_id,
  f.title,
  a.actor_id,
  a.first_name,
  a.last_name,
  r.actor_rank
FROM
  film f
JOIN
  film_actor fa ON f.film_id = fa.film_id
JOIN
  actor a ON fa.actor_id = a.actor_id
JOIN
  actor_ranks r ON a.actor_id = r.actor_id;


SELECT
  film_id,
  title,
  actor_id,
  first_name,
  last_name,
  film_actor_rank
FROM
  (SELECT
    film_id,
    title,
    actor_id,
    first_name,
    last_name,
    ROW_NUMBER() OVER (PARTITION BY film_id ORDER BY actor_rank DESC) as film_actor_rank
  FROM
    film_actor_ranks) x
WHERE
  film_actor_rank = 1;

