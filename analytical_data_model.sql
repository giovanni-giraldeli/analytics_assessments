-------------------------------
---- ANALYTICAL DATA MODEL ----
-------------------------------

CREATE TABLE sales_funnel_fact AS
WITH bounces AS (
SELECT
	"session",
	MIN(event_date) AS min_event_date,
	COUNT(*) AS events_count
FROM
	event_source
GROUP BY
	"session"
)
, landing_page AS (
SELECT
	b."session",
	MIN(es.page_type) AS landing_page_type -- Eliminating concurrent events
FROM
	bounces AS b
LEFT JOIN
	event_source AS es
		ON es."session" = b."session"
		AND es.event_date = b.min_event_date
GROUP BY
	b."session"
)
, unique_values_count AS (
SELECT
	DATE(event_date) AS event_date,
	COUNT(DISTINCT CASE WHEN product = 0 THEN NULL ELSE product END ) AS unique_products_count,
	COUNT(DISTINCT "user") AS unique_users_count,
	COUNT(DISTINCT "session") AS unique_sessions_count,
	COUNT(DISTINCT CASE WHEN event_type = 'order' THEN "user" ELSE NULL END ) AS order_unique_users_count
FROM
	event_source
GROUP BY
	DATE(event_date)
)
SELECT
	ROW_NUMBER() OVER (ORDER BY 
		DATE(es.event_date),
		es.page_type,
		lp.landing_page_type,
		es.event_type
	) AS sales_funnel_sk,
	DATE(es.event_date) AS event_date,
	es.page_type,
	lp.landing_page_type,
	es.event_type,
	COUNT(*) AS events_count,
	COUNT( DISTINCT es."session" ) AS session_count,
	SUM( CASE WHEN es.event_type = 'page_view' THEN 1 ELSE 0 END ) AS page_views_count,
	SUM( CASE WHEN es.product = 0 THEN 0 ELSE 1 END ) AS products_events_count,
	SUM( CASE WHEN es.event_type = 'order' THEN 1 ELSE 0 END ) AS order_events_count,
	SUM( CASE WHEN b.events_count = 1 AND es.event_type = 'page_view' THEN 1 ELSE 0 END ) AS bounces_count,
	AVG(uvc.unique_sessions_count) AS unique_sessions_count,
	AVG(uvc.unique_users_count) AS unique_users_count,
	AVG(uvc.unique_products_count) AS unique_products_count,
	SUM(
		SUM( CASE WHEN es.event_type = 'order' THEN 1 ELSE 0 END )
	) OVER ( PARTITION BY DATE(es.event_date) ) AS unique_orders_count,
	AVG(uvc.order_unique_users_count) AS order_unique_users_count
FROM
	event_source AS es
LEFT JOIN
	bounces AS b
		ON b."session" = es."session"
LEFT JOIN
	landing_page AS lp
		ON lp."session" = es."session"
LEFT JOIN
	unique_values_count AS uvc
		ON uvc.event_date = DATE(es.event_date)
GROUP BY
	DATE(es.event_date),
	es.page_type,
	lp.landing_page_type,
	es.event_type
;

SELECT COUNT(*) FROM sales_funnel_fact;

SELECT * FROM sales_funnel_fact sff ;

SELECT page_type, event_type, COUNT(*) FROM event_source GROUP BY 1,2 ORDER BY 1,2;