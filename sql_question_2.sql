----------------------------------------
-- CANDIDATE GIOVANNI FARIA GIRALDELI --
---------- REFERENCE 2024-12 -----------
----------------------------------------

------------------
-- POSTGRES SQL --
--- QUESTION 2 ---
------------------

-- Creating the target dates for the analysis
with date_series as (
	select date(series_date) as series_date from generate_series('2017-01-01', '2017-12-31', interval '1' day) as series_date
)
-- Mapping unique sellers that had orders during 2017
, unique_sellers as (
	select
		oi.seller_id,
		date(min(o.order_purchase_timestamp)) as min_purchase_date -- First order date during 2017
	from
		orders as o
	left join
		order_items as oi
			on o.order_id = oi.order_id
	cross join
		date_series as ds
	where
		oi.seller_id is not null -- Filtering NULL values for seller_id
		and extract(year from date(o.order_purchase_timestamp)) = 2017 -- Filtering only 2017
	group by
		oi.seller_id
)
-- Joining the targeted dates for every seller starting at the min_purchase_date
-- This script will create a row for every day for each seller starting at the first order filled in 2017
, sellers_dates as (
	select
		ds.series_date,
		us.seller_id
	from
		date_series as ds
	left join
		unique_sellers as us
			on us.min_purchase_date <= ds.series_date
)
-- Mapping how many orders each seller has received daily
, sellers_orders as (
	select
		date(o.order_purchase_timestamp) as date_ref,
		oi.seller_id,
		count(1) as orders_count
	from
		orders as o
	left join
		order_items as oi
			on o.order_id = oi.order_id
	group by
		date(o.order_purchase_timestamp),
		oi.seller_id
)
-- Mapping sellers daily behavior for each targeted date to cascade to monthly and weekly indicators
-- This step is specially important, because it will allow us to input 0 orders in weeks that sellers didn't sell anything,
-- enabling calculating accurately the weekly active sellers
, daily_orders as (
	select
		sd.series_date as date_ref,
		sd.seller_id,
		so.orders_count
	from
		sellers_dates as sd
	left join
		sellers_orders as so
			on so.date_ref = sd.series_date
			and so.seller_id = sd.seller_id
)
-- Grouping statistics for sellers monthly behavior
, monthly_orders as (
	select
		date(date_trunc('month', date_ref)) as month_ref, -- Converting dates to months
		seller_id,
		count(orders_count) as days_active, -- Mapping how many days the seller had an order during the month
		coalesce(sum(orders_count), 0) as monthly_orders -- Mapping how many orders the seller had in total during the month
	from
		daily_orders
	group by
		date(date_trunc('month', date_ref)),
		seller_id
)
-- Grouping statistics for sellers monthly behavior
, weekly_orders as (
	select
		date(date_trunc('week', date_ref)) as week_ref, -- Converting dates to weeks
		seller_id,
		coalesce(sum(orders_count), 0) as orders_count -- Mapping how many orders the seller had in total during the week
	from
		daily_orders
	group by
		date(date_trunc('week', date_ref)),
		seller_id
)
-- Converting weekly statistics to monthly statistics to avoid fan-out the data
-- Here we can have some months mismatch due to the week start reference
-- E.g. an order filled at 2017-03-02 has the week starting at 2017-02-27 and it will be considered in 2017-03 for daily and monthly, but for 2017-02 in weekly
, weekly_active as (
	select
		date(date_trunc('month', week_ref)) as month_ref,
		seller_id,
		avg(orders_count) as avg_weekly_count
	from
		weekly_orders
	group by
		date(date_trunc('month', week_ref)),
		seller_id
)
select
	mo.month_ref,
	sum( -- Summing how many sellers have 25 or more orders in a month to determine monthly active sellers
		case
			when mo.monthly_orders >= 25
				then 1
			else 0
		end
	) as monthly_active_sellers,
	sum( -- Summing how many sellers have 5 or more avg weekly orders in a month to determine weekly active sellers
		case
			when wa.avg_weekly_count >= 5
				then 1
			else 0
		end
	) as weekly_active_sellers,
	sum( -- Summing how many sellers had at least 1 order during the month to determine daily active sellers
		case
			when mo.days_active > 0
				then 1
			else 0
		end
	) as daily_active_sellers
from
	monthly_orders as mo
left join
	weekly_active as wa
		on mo.month_ref = wa.month_ref
		and mo.seller_id = wa.seller_id
group by
	mo.month_ref
order by
	month_ref