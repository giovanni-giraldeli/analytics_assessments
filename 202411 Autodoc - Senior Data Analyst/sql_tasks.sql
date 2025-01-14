--------------------
---- SQL TASK 1 ----
--------------------

CREATE TABLE user_session_fact AS
SELECT
	ROW_NUMBER() OVER (ORDER BY MIN(event_date), "user", "session") AS user_session_sk,
	"user",
	"session",
	MIN(event_date) AS session_start,
	MAX(event_date) AS session_end,
	ROW_NUMBER() OVER (PARTITION BY "user" ORDER BY MIN(event_date)) AS session_number,
	COUNT(*) AS event_count,
	MAX(
		CASE
			WHEN page_type = 'product_page'
			AND event_type = 'page_view'
				THEN 1
			ELSE 0 
		END
	) AS has_visited_product_page
FROM
	event_source
GROUP BY
	"user",
	"session"
;

WITH user_product_view_first_session AS (
SELECT
	"user",
	MAX(
		CASE
			WHEN session_number > 1
			AND has_visited_product_page = 1
				THEN 0
			WHEN session_number = 1
			AND has_visited_product_page = 1
				THEN 1
			ELSE 0
		END
	) AS has_only_product_view_in_first_session
FROM
	user_session_fact
GROUP BY
	"user"
)
SELECT 
	DATE(es.event_date) AS event_date,
	COUNT(*) AS event_count,
	COUNT(DISTINCT es."user") AS user_count,
	COUNT(DISTINCT 
		CASE WHEN upvfs.has_only_product_view_in_first_session = 1 THEN es."user" END
	) AS prod_view_only_first_session_user_count
FROM
	event_source AS es
LEFT JOIN
	user_product_view_first_session AS upvfs
		ON es."user" = upvfs."user"
GROUP BY
	DATE(es.event_date)
;

--------------------
---- SQL TASK 2 ----
--------------------

WITH session_duration AS (
SELECT
	*,
	(
		JULIANDAY(session_end) - JULIANDAY(session_start) -- Calculating the difference in days in SQLite
	) * 24 * 60 AS session_duration_in_minutes -- Times 24 hours in a day, times 60 minutes per hour to get the results in minutes
FROM
	user_session_fact AS usf
)
SELECT
	"user",
	"session",
	event_count,
	ROUND(session_duration_in_minutes, 1) AS session_duration_in_minutes,
	ROUND(event_count / session_duration_in_minutes, 1) AS events_per_minutes,
	ROUND( AVG( event_count / session_duration_in_minutes ) OVER () , 1 ) AS avg_events_per_minutes
FROM
	session_duration
ORDER BY
	event_count DESC
LIMIT 100;

/*
	In this query above we can identify a clear outlier.
	
	The user with the most events in the website also has a pattern to trigger events faster than the average.
	
	Sessions with more events tend to have a lower pace to trigger events if compared with the overall average, but the session with the most events
	has pace even higher than the average.
*/