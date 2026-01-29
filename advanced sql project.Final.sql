--What is the total revenue per month gained from late fees?
DROP TABLE rental_info_detz;
DROP TABLE late_fee_revn;

-- C: Detailed Table
CREATE TABLE IF NOT EXISTS rental_info_detz (
	store_id INT,
	customer_id INT,
	inventory_id INT,
	film_id INT,
	rental_id INT,
	rental_month text,
	payment_id INT UNIQUE,
	rental_rate NUMERIC(5,2),
	charge NUMERIC(5,2),
	return_status BOOLEAN,
	difference NUMERIC(5,2),
	days_late INT,
	last_update TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW() 
);

SELECT * FROM rental_info_detz;

/*B:
transformation nto extract month from timestamp
Provide original code for function(s) in text format
that perform the transformation(s) you identified in part A4.*/
CREATE OR REPLACE FUNCTION get_rental_month(ts TIMESTAMP)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE           -- result never changes for same input
PARALLEL SAFE       -- safe for parallel plans
AS $$
    SELECT TO_CHAR(ts, 'YYYY-MM');
$$;

TRUNCATE rental_info_detz;

/*D: 
Extracting raw data in detailed table
Provide an original SQL query in a text format that 
will extract the raw data needed for the detailed section of 
your report from the source database*/
INSERT INTO rental_info_detz (
    store_id,
    customer_id,
	inventory_id,
	film_id,
    rental_id,
    rental_month,
    payment_id,
    rental_rate,
    charge,
    return_status,
    difference,
    days_late
)
	SELECT
		i.store_id,
		p.customer_id,
		i.inventory_id,
		f.film_id,
	    r.rental_id,
		get_rental_month(r.rental_date) AS rental_month,
	    p.payment_id,
	    f.rental_rate,
	    p.amount  AS charge,
	    (p.amount > f.rental_rate) AS return_status,
		(p.amount - f.rental_rate) AS difference,
	    /* days late, but never negative */
	    GREATEST(
	        (r.return_date::date
	         - (r.rental_date + INTERVAL '1 day' * f.rental_duration)::date),
	        0) AS days_late
	FROM rental    AS r
	JOIN payment   AS p ON p.rental_id    = r.rental_id
	JOIN inventory AS i ON i.inventory_id = r.inventory_id
	JOIN film      AS f ON f.film_id      = i.film_id
	WHERE r.return_date IS NOT NULL -- keep only returned rentals
	AND p.amount > f.rental_rate     --query returns only rentals with late fees
;

SELECT DISTINCT(rental_month) FROM rental_info_detz;
SELECT * FROM rental_info_detz LIMIT 10;

-- C:SUMMARY TABLE
CREATE TABLE IF NOT EXISTS late_fee_revn (
	rental_month TEXT UNIQUE,
	total_late_rev NUMERIC(10,2),
	late_rentals INT,
	last_update TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

SELECT * FROM late_fee_revn;
 
INSERT INTO late_fee_revn(
	rental_month, 
	total_late_rev, 
	late_rentals
)
	SELECT
	    rental_month,
	    SUM(difference) AS total_late_rev,
	    COUNT(*) AS late_rentals
	FROM   rental_info_detz
	GROUP  BY rental_month
	ORDER BY rental_month
	ON CONFLICT (rental_month) DO UPDATE
	SET total_late_rev = EXCLUDED.total_late_rev, --Overwrites existing month’s total_late_revn with new calculated total
	    late_rentals   = EXCLUDED.late_rentals, -- same concept as total_late_revn
	    last_update    = NOW()
;

SELECT * FROM late_fee_revn;

--E: 
--Trigger to update late_fee_revn table when rental_info_detz table updates
CREATE OR REPLACE FUNCTION insert_rental_info_detz()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
DELETE FROM late_fee_revn;
INSERT INTO late_fee_revn (
	rental_month, 
	total_late_rev, 
	late_rentals
)
	SELECT
	    rental_month,
	    SUM(difference) AS total_late_rev,
	    COUNT(*) AS late_rentals
	FROM   rental_info_detz
	GROUP  BY rental_month
	ON CONFLICT (rental_month) DO UPDATE
	SET total_late_rev = EXCLUDED.total_late_rev, --Overwrites existing month’s total_late_revn with new calculated total
	    late_rentals   = EXCLUDED.late_rentals, -- same concept as total_late_revn
	    last_update    = NOW()
;
	RETURN NEW;
END;
$$;

CREATE TRIGGER new_rental_info_detz
	AFTER INSERT
	ON rental_info_detz
	FOR EACH STATEMENT
	EXECUTE PROCEDURE insert_rental_info_detz()
;

--test trigger for insert on rental_info_detz
DELETE FROM rental_info_detz WHERE rental_month ='2005-09';
INSERT INTO rental_info_detz(
  	store_id,
    customer_id,
	inventory_id,
	film_id,
    rental_id,
    rental_month,
    payment_id,
    rental_rate,
    charge,
    return_status,
    difference,
    days_late
)
VALUES 
	(2,341,1,1,16050,'2005-09',32099,0.99,2.99,TRUE,2.00,2);

--TEST INSERT TRIGGER 
SELECT rental_month, total_late_rev, late_rentals FROM late_fee_revn ORDER BY rental_month;


--F: 
--Automation
CREATE OR REPLACE PROCEDURE update_rental_info_n_late_fee()
LANGUAGE plpgsql
AS $$
BEGIN
DELETE FROM rental_info_detz;
DELETE FROM late_fee_revn;

INSERT INTO rental_info_detz (
    store_id,
    customer_id,
	inventory_id,
	film_id,
    rental_id,
    rental_month,
    payment_id,
    rental_rate,
    charge,
    return_status,
    difference,
    days_late
)
	SELECT
		i.store_id,
		p.customer_id,
		i.inventory_id,
		f.film_id,
	    r.rental_id,
		get_rental_month(r.rental_date) AS rental_month,
	    p.payment_id,
	    f.rental_rate,
	    p.amount  AS charge,
	    (p.amount > f.rental_rate) AS return_status,
		(p.amount - f.rental_rate) AS difference,
	    /* days late, but never negative */
	    GREATEST(
	        (r.return_date::date
	         - (r.rental_date + INTERVAL '1 day' * f.rental_duration)::date),
	        0) AS days_late
	FROM rental r
	JOIN payment   AS p ON p.rental_id    = r.rental_id
	JOIN inventory AS i ON i.inventory_id = r.inventory_id
	JOIN film      AS f ON f.film_id      = i.film_id
	WHERE r.return_date IS NOT NULL -- keep only returned rentals
	AND p.amount > f.rental_rate     --query returns only rentals with late fees
;
INSERT INTO late_fee_revn(
	rental_month, 
	total_late_rev, 
	late_rentals
)
	SELECT
	    rental_month,
	    SUM(difference) AS total_late_rev,
	    COUNT(*) AS late_rentals
	FROM rental_info_detz
	GROUP  BY rental_month
	ORDER BY rental_month
	ON CONFLICT (rental_month) DO UPDATE
	SET 
	    total_late_rev = EXCLUDED.total_late_rev,
	    late_rentals = EXCLUDED.late_rentals
;
RETURN;
END;
$$;

CALL update_rental_info_n_late_fee();
select * from rental_info_detz;
select * from late_fee_revn;
