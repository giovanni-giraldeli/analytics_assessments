----------------------------------------
-- CANDIDATE GIOVANNI FARIA GIRALDELI --
---------- REFERENCE 2024-12 -----------
----------------------------------------

------------------
-- POSTGRES SQL --
--- QUESTION 3 ---
------------------

-- Creating the target dates for the analysis
with date_series as (
	select date(series_date) as series_date from generate_series('2017-01-01', '2017-12-31', interval '1' day) as series_date
)
-- Converting dates to weeks and filtering only weeks starting in 2017
, week_series as (
	select
		date(date_trunc('week', series_date)) as series_week
	from
		date_series
	where
		extract( year from date_trunc('week', series_date) ) = 2017
	group by
		date(date_trunc('week', series_date))
)
-- Ranking categories by their GMV in November/2017
, category_rank as (
	select
		p.product_category_name,
		count(1) as items_sold_count,
		-- Ranking the categories by quantity of items sold and prioritizing alphabetically in case of a tie
		row_number() over (order by count(1) desc, p.product_category_name) as ranking_position
	from
		products as p
	left join
		order_items as oi
			on p.product_id = oi.product_id
	left join
		orders as o
			on o.order_id = oi.order_id
	where
		nullif(p.product_category_name, '') is not null -- Filtering out NULL values in the category names
		and nullif(o.order_id, '') is not null -- Filtering out orders without match in the orders table
		and date_trunc('month', date(o.order_purchase_timestamp)) = '2017-11-01' -- Considering only orders purchased at November/2017
	group by
		p.product_category_name
)
-- Joining the targeted weeks with the category names
, category_weeks as (
	select
		ws.series_week as week_ref,
		cr.product_category_name,
		cr.ranking_position
	from
		week_series as ws
	cross join
		category_rank as cr
	where
		ranking_position <= 3 -- Filtering only the 3 best performing categories
)
-- Mapping the weekly GMV by product category names
, weekly_items_sold as (
	select
		date(date_trunc('week', date(o.order_purchase_timestamp))) as week_ref,
		p.product_category_name,
		sum(oi.price) as gmv
	from
		products as p
	left join
		order_items as oi
			on p.product_id = oi.product_id
	left join
		orders as o
			on o.order_id = oi.order_id
	group by
		date(date_trunc('week', date(o.order_purchase_timestamp))),
		p.product_category_name
)
-- Aggregating the weekly GMV with the targeted weeks
-- I'm using this CTE to retrieve only 1 row for each week and pivoting each category as columns
, agg_weekly_metrics as (
	select
		cw.week_ref,
		max(
			case
				when cw.ranking_position = 1
					then cw.product_category_name
			end
		) as category_name_rank_1,
		max(
			case
				when cw.ranking_position = 1
					then coalesce(wis.gmv, 0)
			end
		) as gmv_rank_1,
		max(
			case
				when cw.ranking_position = 2
					then cw.product_category_name
			end
		) as category_name_rank_2,
		max(
			case
				when cw.ranking_position = 2
					then coalesce(wis.gmv, 0)
			end
		) as gmv_rank_2,
		max(
			case
				when cw.ranking_position = 3
					then cw.product_category_name
			end
		) as category_name_rank_3,
		max(
			case
				when cw.ranking_position = 3
					then coalesce(wis.gmv, 0)
			end
		) as gmv_rank_3
	from
		category_weeks as cw
	left join
		weekly_items_sold as wis
			on wis.week_ref = cw.week_ref
			and wis.product_category_name = cw.product_category_name
	group by
		cw.week_ref
)
select
	week_ref,
	category_name_rank_1,
	category_name_rank_2,
	category_name_rank_3,
	round(gmv_rank_1::numeric, 2) as gmv_rank_1,
	round(gmv_rank_2::numeric, 2) as gmv_rank_2,
	round(gmv_rank_3::numeric, 2) as gmv_rank_3,
	round( sum(gmv_rank_1) over (order by week_ref)::numeric, 2 ) as gmv_running_total_rank_1,
	round( sum(gmv_rank_2) over (order by week_ref)::numeric, 2 ) as gmv_running_total_rank_2,
	round( sum(gmv_rank_3) over (order by week_ref)::numeric, 2 ) as gmv_running_total_rank_3,
	round( 
		( ( gmv_rank_1 / nullif( lag(gmv_rank_1) over (order by week_ref), 0 ) ) - 1 )::numeric,
	3) as gmv_wow_rank_1,
	round( 
		( ( gmv_rank_2 / nullif( lag(gmv_rank_2) over (order by week_ref), 0 ) ) - 1 )::numeric,
	3) as gmv_wow_rank_2,
	round( 
		( ( gmv_rank_3 / nullif( lag(gmv_rank_3) over (order by week_ref), 0 ) ) - 1 )::numeric,
	3) as gmv_wow_rank_3
from
	agg_weekly_metrics
order by
	week_ref