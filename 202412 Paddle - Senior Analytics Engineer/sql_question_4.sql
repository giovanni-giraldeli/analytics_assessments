----------------------------------------
-- CANDIDATE GIOVANNI FARIA GIRALDELI --
---------- REFERENCE 2024-12 -----------
----------------------------------------

------------------
-- POSTGRES SQL --
--- QUESTION 4 ---
------------------

-- Creating table and enforcing checks to improve data quality
create table orders_delivery_cube (
	order_id text,
	product_id text not null,
	seller_id text not null,
	customer_city text not null,
	customer_state text not null,
	order_delivered_carrier_date date not null,
	order_delivered_customer_date date not null,
	items_sold integer not null,
	gmv real not null,
	carrier_delivery_leadtime integer not null,
	primary key ( order_id, product_id, seller_id ),
	constraint customer_state_check check (
		customer_state in (
			'AC',
			'AL',
			'AM',
			'AP',
			'BA',
			'CE',
			'DF',
			'ES',
			'GO',
			'MA',
			'MG',
			'MS',
			'MT',
			'PA',
			'PB',
			'PE',
			'PI',
			'PR',
			'RJ',
			'RN',
			'RO',
			'RR',
			'RS',
			'SC',
			'SE',
			'SP',
			'TO'
		)
	)
);

-- Starting a transaction to enforce ACID properties
begin transaction;

-- Clearing the table before inserting current values
truncate table orders_delivery_cube;

-- Inserting values into the table with the suggested metrics
insert into orders_delivery_cube
-- Analyzing the payment methods used for each order
with payment_method as (
	select
		order_id,
		array_agg(payment_type) as payment_methods_used,
		count(1) as payment_methods_used_count,
		max(
			case
				when payment_type in ('credit_card', 'debit_card')
					then 1
				else 0
			end
		) as has_used_card,
		max(
			case
				when payment_type not in ('credit_card', 'debit_card')
					then 1
				else 0
			end
		) as has_used_other_than_card
	from
		order_payments
	group by
		order_id
)
select
	o.order_id,
	oi.product_id,
	oi.seller_id,
	c.customer_city,
	c.customer_state,
	date(order_delivered_carrier_date) as order_delivered_carrier_date,
	date(order_delivered_customer_date) as order_delivered_customer_date,
	count(1) as items_sold,
	sum(oi.price) as gmv,
	date(order_delivered_customer_date) - date(order_delivered_carrier_date) as carrier_delivery_leadtime
from
	orders as o
inner join
	payment_method as pm
		on pm.order_id = o.order_id
		and pm.has_used_card = 1 -- Considering orders that used card as their payment method
		and pm.has_used_other_than_card = 0 -- Filtering out orders that have payment methods other than cards
inner join
	order_items as oi
		on oi.order_id = o.order_id
inner join
	customers as c
		on c.customer_id = o.customer_id
where
	extract( year from date(nullif(o.order_delivered_carrier_date, '') ) ) = 2018 -- Orders shipped in 2018
	and extract( year from date(nullif(o.order_delivered_customer_date, '') ) ) = 2018 -- Orders delivered in 2018
group by
	date(order_delivered_carrier_date),
	c.customer_city,
	c.customer_state,
	o.order_id,
	oi.product_id,
	oi.seller_id
;

-- Ending the transaction
commit;

-- Verifying the results from the new table
select * from orders_delivery_cube limit 100;

----------------------
-- Analysis example --
----------------------
-- This wasn't a requirement, but I'll display on how we can generate for analytics with this data model

-- Calculating monthly metrics
with monthly_metrics as (
	select
		date(date_trunc('month', order_delivered_carrier_date)) as cohort_month,
		customer_state,
		round( avg(carrier_delivery_leadtime), 1) as avg_carrier_delivery_leadtime,
		count(distinct order_id) as orders_count,
		count(1) as distinct_items_delivered,
		sum(items_sold) as items_delivered,
		round( count(1) * 1.00 / count(distinct order_id), 2) as distinct_items_per_order,
		round( sum(items_sold) * 1.00 / count(distinct order_id), 2) as items_per_order,
		sum(gmv) as gmv
	from
		orders_delivery_cube
	group by
		date(date_trunc('month', order_delivered_carrier_date)),
		customer_state
)
-- Ranking the metrics calculated previously
, ranked_metrics as (
	select
		cohort_month,
		customer_state,
		dense_rank() over (partition by cohort_month order by avg_carrier_delivery_leadtime) as leadtime_ranking,
		dense_rank() over (partition by cohort_month order by orders_count desc) as orders_count_ranking,
		dense_rank() over (partition by cohort_month order by gmv desc) as gmv_ranking,
		avg_carrier_delivery_leadtime,
		orders_count,
		distinct_items_delivered,
		items_delivered,
		distinct_items_per_order,
		items_per_order,
		gmv
	from
		monthly_metrics
	order by
		cohort_month,
		avg_carrier_delivery_leadtime
)
-- Storing the past rankings to compare with the current ones
, ranking_evolution as (
	select
		cohort_month,
		customer_state,
		leadtime_ranking,
		orders_count_ranking,
		gmv_ranking,
		array_agg( leadtime_ranking ) over (partition by customer_state order by cohort_month) as leadtime_ranking_evolution,
		array_agg( orders_count_ranking ) over (partition by customer_state order by cohort_month) as orders_count_ranking_evolution,
		array_agg( gmv_ranking ) over (partition by customer_state order by cohort_month) as gmv_ranking_evolution,
		avg_carrier_delivery_leadtime,
		orders_count,
		distinct_items_delivered,
		items_delivered,
		distinct_items_per_order,
		items_per_order,
		gmv
	from
		ranked_metrics
)
select
	cohort_month,
	customer_state,
	leadtime_ranking,
	orders_count_ranking,
	gmv_ranking,
	leadtime_ranking_evolution[1] - leadtime_ranking as leadtime_ranking_improvement,
	orders_count_ranking_evolution[1] - orders_count_ranking as orders_count_ranking_improvement,
	gmv_ranking_evolution[1] - gmv_ranking as gmv_ranking_improvement,
	leadtime_ranking_evolution,
	orders_count_ranking_evolution,
	gmv_ranking_evolution,
	avg_carrier_delivery_leadtime,
	orders_count,
	distinct_items_delivered,
	items_delivered,
	distinct_items_per_order,
	items_per_order,
	gmv
from
	ranking_evolution
where
	cohort_month = '2018-08-01'
order by
	gmv_ranking

-- The 3 states with the most GMV (SP, RJ and MG) have the best leadtimes as well, what looks healthy.
-- We could see that RJ, RS and SC were states with great GMV in the start of 2018 that improved considerably their leadtimes.
-- MT is a state with low GMV, but with good delivery indicators. We could benefit either being more aggressive in marketing in this state
-- or cutting down eventual costs in this area to focus the operation in other more profitable regions, such as BA.
-- BA is one of the state with GMV, but it's only in the 10th position in leadtime. From the start of 2018 until August, it lost 2 positions (from 5th to 7th)
-- in the GMV ranking, while the leadtime remained in the same position.