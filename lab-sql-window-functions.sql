USE sakila;

-- Rank films by their length and create an output table that includes the title
-- , length, and rank columns only. Filter out any rows with null or zero values in the length column.

SELECT title, length, RANK() OVER(partition by length ORDER BY title) 'Ranks'
FROM film
WHERE length != null or length != 0;

-- fixed code

SELECT title, length, RANK() OVER (ORDER BY length DESC, title) as 'Ranks'
FROM film
WHERE length IS NOT NULL AND length != 0;

-- Rank films by length within the rating category and create an output table 
-- that includes the title, length, rating and rank columns only. Filter out any rows 
-- with null or zero values in the length column.

SELECT title, length, rating, RANK() OVER(partition by length ORDER BY rating) as 'ranks'
FROM film
WHERE length != null or length != 0;

-- fixed code

SELECT title, length, rating, RANK() OVER (PARTITION BY rating ORDER BY length DESC) as 'ranks'
FROM film
WHERE length IS NOT NULL AND length != 0;

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
CREATE TEMPORARY TABLE actor_film_counts
SELECT actor_id, COUNT(*) AS film_count
FROM film_actor
GROUP BY actor_id;


CREATE VIEW film_actor_ranks AS
WITH actor_ranks AS (
  SELECT
    actor_id,
    film_count,
    RANK() OVER (ORDER BY film_count DESC) as actor_rank
  FROM (
	SELECT actor_id, COUNT(*) AS film_count
	FROM film_actor
	GROUP BY actor_id) as actor_film_counts
    )
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
  
  

-- Challenge 2

-- Step 1. Retrieve the number of monthly active customers, i.e., the number of unique customers
--  who rented a movie in each month.

select monthname(rental_date) as month, count(distinct customer_id) as num_of_customer
from rental
group by monthname(rental_date);

-- Step 2. Retrieve the number of active users in the previous month.

SELECT 
  m.month, 
  COUNT(DISTINCT m.customer_id) AS current_month_customers, 
  COALESCE(pm.prev_month_customers, 0) AS previous_month_customers
FROM 
  (
    SELECT 
      MONTHNAME(rental_date) AS month, 
      customer_id
    FROM 
      rental
  ) AS m
  LEFT JOIN 
  (
    SELECT 
      MONTHNAME(m.rental_date) AS month, 
      COUNT(DISTINCT m.customer_id) AS prev_month_customers
    FROM 
      (
        SELECT 
          DATE_SUB(r.rental_date, INTERVAL 1 MONTH) AS rental_date, 
          r.customer_id
        FROM 
          rental r
      ) AS m
    GROUP BY 
      MONTHNAME(m.rental_date)
  ) AS pm
  ON m.month = pm.month
GROUP BY 
  m.month, pm.prev_month_customers
ORDER BY 
  m.month;
  
  
-- Step 3. Calculate the percentage change in the number of active customers between the current 
-- and previous month.

WITH monthly_customers AS (
  SELECT 
    MONTHNAME(rental_date) AS month, 
    COUNT(DISTINCT customer_id) AS current_month_customers
  FROM 
    rental
  GROUP BY 
    MONTHNAME(rental_date)
)
SELECT 
  month, 
  current_month_customers, 
  LAG(current_month_customers) OVER (ORDER BY month ASC) AS previous_month_customers,
  (current_month_customers - LAG(current_month_customers) OVER (ORDER BY month ASC)) / LAG(current_month_customers) OVER (ORDER BY month ASC) * 100 AS percentage_change
FROM 
  monthly_customers
ORDER BY 
  month;

-- Step 4. Calculate the number of retained customers every month, i.e., customers who rented 
-- movies in the current and previous months.

WITH monthly_customers AS (
  SELECT 
    MONTHNAME(rental_date) AS month, 
    customer_id
  FROM 
    rental
  GROUP BY 
    MONTHNAME(rental_date), customer_id
),
previous_month_customers AS (
  SELECT 
    MONTHNAME(DATE_SUB(r.rental_date, INTERVAL 1 MONTH)) AS month, 
    r.customer_id
  FROM 
    rental r
  GROUP BY 
    MONTHNAME(DATE_SUB(r.rental_date, INTERVAL 1 MONTH)), r.customer_id
)
SELECT 
  m.month, 
  COUNT(DISTINCT m.customer_id) AS retained_customers
FROM 
  monthly_customers m
  JOIN previous_month_customers pm ON m.month = pm.month AND m.customer_id = pm.customer_id
GROUP BY 
  m.month
ORDER BY 
  m.month;
