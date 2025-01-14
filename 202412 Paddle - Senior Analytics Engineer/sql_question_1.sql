----------------------------------------
-- CANDIDATE GIOVANNI FARIA GIRALDELI --
---------- REFERENCE 2024-12 -----------
----------------------------------------

------------------
-- POSTGRES SQL --
--- QUESTION 1 ---
------------------

/*
TABLE CUSTOMERS
This table contains information about the client region and map their address and their unique key.
It's interesting to see that for every order a new customer_id is created and then it's assigned the unique key to them.
This can be stated by analyzing the provided ERD and the cardinality from the relationships.
To confirm this statement, I'll calculate in the first query the fillment and the uniqueness of the customer_id and customer_unique_id.
I'll take advantage to check the fillment of the other columns as well in the same query. I won't check the uniqueness of these other columns,
since it's expected that multiple customers live in the same city/state or even in the same ZIP code.

After inspecting the results, we could confirm that customer_id is indeed the primary key of the table,
while customer_unique_id doesn't have NULLs, but it has some duplicates.

We could analyze that the customer_state is a low-cardinality column (27 distinct values), so we can add a constraint in it to ENUM values.

All the other columns are 100% filled.

Based on this scenario, I'll create the following constraints:
-- PRIMARY KEY for customer_id
-- NOT NULLs constraints for every column
-- CHECK for acceptable values for the column customer_state
*/

-- Describing table
with agg as (
select 
	count(1) as total_count, -- Count of total registers
	count(nullif(customer_id,'')) as count_pk, -- Count of existing customer_id
	count(distinct nullif(customer_id,'')) as count_dist_pk, -- Count of distinct customer_id
	count(nullif(customer_unique_id,'')) as count_unique_id, -- Count of existing customer_unique_id
	count(distinct nullif(customer_unique_id,'')) as count_dist_unique_id, -- Count of distinct customer_unique_id
	count(customer_zip_code_prefix) as count_zip_code,
	count(nullif(customer_city,'')) as count_city,
	count(distinct nullif(customer_city,'')) as count_dist_city,
	count(nullif(customer_state,'')) as count_state,
	count(distinct nullif(customer_state,'')) as count_dist_state
from 
	customers
)
select
	total_count, -- 99,441
	count_pk,
	count_dist_pk,
	count_unique_id,
	count_dist_unique_id,
	count_zip_code,
	count_city,
	count_dist_city,
	count_state,
	count_dist_state,
	count_pk * 1.0 / total_count as perc_fill_pk, -- customer_id fillment >> 100%
	count_dist_pk * 1.0 / total_count as perc_dist_pk, -- customer_id uniqueness >> 100%
	count_unique_id * 1.0 / total_count as perc_fill_unique_id, -- customer_unique_id fillment >> 100%
	count_dist_unique_id * 1.0 / total_count as perc_dist_unique_id, -- customer_unique_id uniqueness >> 96.6%
	count_zip_code * 1.0 / total_count as perc_fill_zip_code, -- customer_zip_code_prefix fillment >> 100%
	count_city * 1.0 / total_count as perc_fill_city, -- customer_city fillment >> 100%
	count_state * 1.0 / total_count as perc_fill_state -- customer_state fillment >> 100%
from
	agg
;

-- Verifying the distinct values from customer_state to create the CHECK constraint
select distinct
	customer_state
from
	customers
order by
	customer_state
;

-- Adding table constraints to improve data quality
alter table customers
	add constraint customers_pk primary key ( customer_id ),
	alter customer_unique_id set not null,
	alter customer_zip_code_prefix set not null,
	alter customer_city set not null,
	alter customer_state set not null,
	add constraint customer_state_check check (
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
;

/*
TABLE ORDER_ITEMS
The first thing that surfaced was the type of the column order_item_id. Clearly it could be an integer.
It looks like that the primary key should be the order_id||order_item_id or order_id||product_id, since this seems to be the detailment of the order.
Maybe the seller_id could be part of the primary as well, since I imagine that the same product could be sold by multiple sellers.
I'll do basic fillment checks and test some key combinations.

Before start the analysis, the cardinality of the relationship with the table orders isn't what I expected.
In the diagram, it's indicating that an order can have 0 rows in the order_items. I'll check this before going further.
In fact, there are some orders that don't exist in the order_items, but there's 1 order that doesn't exist in the table orders.
This could be one deletion anomaly, since the order exist in one auxiliar table and doesn't exist main table of the ERD.

Additionally, the column order_item_id identifies the item placement in the cart, so I expect that every order begins with 1.
Indeed it was confirmed that every order_id has a order_item_id = 1.

Every column is 100% filled, but the price.
There are 2 records missing in this column. In both records the freight value is there, so it looks like it's an insertion anomaly.
-- order_id IN ('0e4a4de1d15b6df8d4335fb2db61667a', '17ca01c238bad17cd6c09179888b6d7f') -->> pair values [ 23.48 , 150 ]
Both orders have more than 1 item purchased, so it could be an offer to get some product free in the cart when buying multiple products.
In fact all items bought were of the same product in each cart.

To check this hypothesis, I've compared the values with the order_payments table.
The result was that there's a difference in the values and the value missing is exactly of 1 item in each cart.
In other words, there's a high probability that there was an insertion anomaly when registering these items.
We can fix in the analytical environment, but the best solution would request the fix upstream to have the transactional layer accurate.

This anomaly above made explicit that product_id and seller_id can have duplicates in the same order if the customer buys multiples of the same item in a single order.
The conclusion is that the primary key should be order_id||order_item_id.

Based on this scenario, these are the take aways:
-- Request to update NULL values in the column price
---- price = 23.48 for order_id = '0e4a4de1d15b6df8d4335fb2db61667a' , price = 150 for order_id = '17ca01c238bad17cd6c09179888b6d7f'
-- PRIMARY KEY for the columns ( order_id, order_item_id )
-- NOT NULLs constraints for every column
*/

-- Checking relationship cardinality
with order_items_ids as (
select
	order_id
from
	order_items
group by
	order_id
)
select
	case 
		when (o.order_id is null and oi.order_id is null)
			then 'NULL id in both tables'
		when oi.order_id is null
			then 'NULL id in order_items'
		when o.order_id is null
			then 'NULL id in orders'
		else 'Existing id in both tables'
	end as type,
	count(1) as count_order_id
from
	orders as o
full outer join
	order_items as oi
		on o.order_id = oi.order_id
group by 1
order by 2 desc
;

-- Checking if order_item_id begins with 1 for every order
with min_order_item_id as (
	select
		order_id,
		min(order_item_id) as min_order_item_id
	from
		order_items
	group by
		order_id
)
select
	min_order_item_id, -- Only has 1 as expected
	count(1) as cnt
from
	min_order_item_id
group by
	min_order_item_id
order by
	min_order_item_id

-- Describing table
with agg as (
select
	count(1) as total_count,
	count(nullif(order_id,'')) as count_order_id,
	count(nullif(order_item_id,'')) as count_order_item_id,
	count(nullif(product_id,'')) as count_product_id,
	count(nullif(seller_id,'')) as count_seller_id,
	count(nullif(shipping_limit_date,'')) as count_ship,
	count(price) as count_price,
	count(freight_value) as count_freight,
	count(distinct order_id||order_item_id) as key_test_1_count,
	count(distinct order_id||order_item_id||product_id||seller_id) as key_test_2_count
from 
	order_items
)
select
	total_count, -- 112,650
	count_order_id,
	count_order_item_id,
	count_product_id,
	count_seller_id,
	count_ship,
	count_price,
	count_freight,
	count_order_id * 1.0 / total_count as perc_fill_order_id, -- order_id fillment >> 100%
	count_order_item_id * 1.0 / total_count as perc_fill_count_order_item_id, -- order_item_id fillment >> 100%
	count_product_id * 1.0 / total_count as perc_fill_count_product_id, -- product_id fillment >> 100%
	count_seller_id * 1.0 / total_count as perc_fill_count_seller_id, -- seller_id fillment >> 100%
	count_ship * 1.0 / total_count as perc_fill_count_ship, -- shipping_limit_date fillment >> 100%
	count_price * 1.0 / total_count as perc_fill_count_price, -- price fillment >> 99.998%
	count_freight * 1.0 / total_count as perc_fill_count_freight, -- freight_value fillment >> 100%
	key_test_1_count * 1.0 / total_count as perc_dist_key1,
	key_test_2_count * 1.0 / total_count as perc_dist_key2
from
	agg
;

with oi_prices as ( -- Checking the total price for orders that have NULL in the column price in the table order_items
select 
	oi.order_id,
	sum(oi.price) + sum(oi.freight_value) as total_price_order_items
from
	order_items as oi
where
	oi.order_id IN ('0e4a4de1d15b6df8d4335fb2db61667a', '17ca01c238bad17cd6c09179888b6d7f')
group by
	oi.order_id
)
, op_prices as ( -- Checking the total price in the table order_payments
select
	op.order_id,
	sum(op.payment_value) as total_price_order_payments
from
	order_payments as op
group by
	op.order_id
)
select -- Comparing the prices between different tables to analyze if the NULL is expected or not
	oi.order_id,
	oi.total_price_order_items, -- 130.84, 848.10
	op.total_price_order_payments -- 154.32, 998.10 >> difference = 23.48, 150
from
	oi_prices as oi
inner join
	op_prices as op
		on op.order_id = oi.order_id
;

update order_items -- Inputing the missing value to match the table order_payments >> in a real scenario I'd request it to be inserted in the transactional layer
	set price = 23.48 where order_id = '0e4a4de1d15b6df8d4335fb2db61667a' and price is null
;
update order_items -- Inputing the missing value to match the table order_payments >> in a real scenario I'd request it to be inserted in the transactional layer
	set price = 150 where order_id = '17ca01c238bad17cd6c09179888b6d7f' and price is null
;

-- Adding table constraints to improve data quality
alter table order_items
	add constraint order_items_pk primary key ( order_id, order_item_id ),
	alter product_id set not null,
	alter seller_id set not null,
	alter shipping_limit_date set not null,
	alter price set not null,
	alter freight_value set not null
;

/*
TABLE ORDER_PAYMENTS
The cardinality of the relationship in the ERD is not what I expected at first. Basically it says that an order can have multiple rows in the order_payments.
Before analyzing the data quality, I'll check some cases to understand better how data flows and what are the candidate keys.
Analyzing the pattern, seems that vouchers are considered as a payment_type and every voucher that the customer uses is assigned as a different payment_type.
The customer can use multiple cards to pay for the transaction as well, so some order_id have multiple cards associated with it.
That said, the primary key should be the order_id||payment_sequential, which is the column that lists the payment_type in the transaction.

It's expected that the payment_sequential starts with 1 for every order_id, since it identifies the placement of a payment method.
Most of the orders (99,360) has 1 as the minimum for the column payment_sequential, but 80 rows has 2 as the minimum value.
Inspecting the values, it's not clear if there was a payment method prior that was deleted or not. This needed to be checked with the upstream data
producers and/or checked the transactional logs to analyze more precisely what happened.

In parallel, the column payment_type could benefit having an ENUM, while the payment_installments seems to be a low cardinality column, which would benefit
of a constraint of max number of installments accepted.
Although payment_sequential is a low-cardinality column as well, the customers can use as many voucher as they have. If there was a clear business rule to
allow customers using a maximum number of voucher in each purchase, then we could create a constraint for that, but since it's not the case then I won't
consider ENUMs for payment_sequential.

The maximum number of installments is 24 and payment_type has 5 possible values (boleto, credit_card, debit_card, not_defined, voucher).
The payment_installment = 0 and payment_type = 'not_defined' are unexpected. I'd try to understand better what these values mean with the upstream data
producers.

Given this scenario, I'll add the following constraints:
-- PRIMARY KEY for the columns ( order_id, payment_sequential )
-- NOT NULLs constraints for every column
-- CHECK acceptable values for payment_type and for payment_installments

Additionally, I'd request help for the upstream data producers to understand better why some orders begin with payment_sequential = 2
*/

-- Verifying order_id's with multiple rows
with row_count as (
select
	*,
	count(1) over (partition by order_id) as row_count
from
	order_payments
)
select
	*
from
	row_count
where
	row_count > 1
order by
	order_id,
	payment_sequential

-- Checking if payment_sequential begins with 1 for every order
with min_payment_sequential as (
	select
		order_id,
		min(payment_sequential) as min_payment_sequential
	from
		order_payments
	group by
		order_id
)
select
	min_payment_sequential, -- Most of them has 1, but 80 rows begins with 2!
	count(1) as cnt
from
	min_payment_sequential
group by
	min_payment_sequential
order by
	min_payment_sequential

-- Veryfing payment_sequential starting with values higher than 1
with min_payment_sequential as (
	select
		order_id,
		min(payment_sequential) as min_payment_sequential
	from
		order_payments
	group by
		order_id
)
select
	*
from
	order_payments as op
inner join
	min_payment_sequential as mps
		on mps.order_id = op.order_id
		and mps.min_payment_sequential > 1
order by
	op.order_id,
	op.payment_sequential

-- Describing table
with agg as (
select
	count(1) as total_count,
	count(nullif(order_id,'')) as order_id_count,
	count(distinct nullif(order_id,'')) as order_id_dist_count,
	count(payment_sequential) as payment_sequential_count,
	count(distinct payment_sequential) as payment_sequential_dist_count,
	count(nullif(payment_type,'')) as payment_type_count,
	count(distinct nullif(payment_type,'')) as payment_type_dist_count,
	count(payment_installments) as installments_count,
	count(distinct payment_installments) as installments_dist_count,
	count(payment_value) as value_count,
	count(distinct order_id || payment_sequential) as key_test_1
from
	order_payments
)
select
	total_count, -- 103,886
	order_id_count,
	order_id_dist_count,
	payment_sequential_count,
	payment_sequential_dist_count, -- 29 distinct values
	payment_type_count,
	payment_type_dist_count, -- 5 distinct values
	installments_count,
	installments_dist_count, -- 24 distinct values
	value_count,
	order_id_count * 1.0 / total_count as perc_fill_order_id, -- order_id fillment >> 100%
	order_id_dist_count * 1.0 / total_count as perc_unique_order_id, -- order_id uniqueness >> 95.7%
	payment_sequential_count * 1.0 / total_count as perc_fill_payment_sequential, -- payment_sequential_id fillment >> 100%
	payment_type_count * 1.0 / total_count as perc_fill_payment_type, -- payment_type fillment >> 100%
	installments_count * 1.0 / total_count as perc_fill_installments, -- payment_installments fillment >> 100%
	value_count * 1.0 / total_count as perc_fill_value, -- payment_value fillment >> 100%
	key_test_1 * 1.0 / total_count as perc_dist_key1 -- key1 uniqueness >> 100%
from
	agg
;

-- Checking values for low-cardinality columns >> payment_type
select
	payment_type, -- (boleto, credit_card, debit_card, not_defined, voucher)
	count(1) as cnt
from
	order_payments
group by
	payment_type
order by
	payment_type
;

-- Checking values for low-cardinality columns >> payment_installments
select
	payment_installments, -- from 0 to 24
	count(1) as cnt
from
	order_payments
group by
	payment_installments
order by
	payment_installments
;

-- Verifying unexpected values
select 
	*
from
	order_payments
where
	payment_installments = 0
	or payment_type = 'not_defined'
;

-- Adding table constraints to improve data quality
alter table order_payments
	add constraint order_payment_pk primary key ( order_id, payment_sequential ),
	alter payment_type set not null,
	alter payment_installments set not null,
	alter payment_value set not null,
	add constraint payment_type_check check ( payment_type in ('boleto', 'credit_card', 'debit_card', 'not_defined', 'voucher') ),
	add constraint payment_installments_check check ( payment_installments between 0 and 24 )
;

/*
TABLE ORDERS
This table already suggests the order_id as the primary key of the table.
I'll check the customer_id as a candidate key as well, since in the table customers we saw that an unique customer could have multiple customer_id.
In parallel, the column order_status seems to have low cardinality, so I'll verify if it's feasible to create an ENUM constraint for it.

Indeed the order_status has low_cardinality, so we may add a constraint in it.
The list of values is the following: ('approved', 'canceled', 'created', 'delivered', 'invoiced', 'processing', 'shipped', 'unavailable')

In parallel, the intermediate dates contain NULLs, so we are limited to add constraints in them.

Given this scenario, I will add the following constraints:
-- PRIMARY KEY for order_id
-- NOT NULL for the columns customer_id, order_status, order_purchase_timestamp, order_estimated_delivery_date
-- CHECK for acceptable values in the column order_status
-- UNIQUE values for the column customer_id

As an iteration, we could create warnings for this table based on the growth of records. This could enable us to detect product unavailability, possible
cyber attacks and eventually some application insertation anomalies.
*/

-- Describing table
with agg as (
select 
	count(1) as total_count,
	count(nullif(order_id,'')) as order_id_count,
	count(distinct nullif(order_id,'')) as order_id_dist_count,
	count(nullif(customer_id,'')) as customer_id_count,
	count(distinct nullif(customer_id,'')) as customer_id_dist_count,
	count(nullif(order_status,'')) as order_status_count,
	count(distinct nullif(order_status,'')) as order_status_dist_count,
	count(nullif(order_purchase_timestamp,'')) as order_purchase_timestamp_count,
	min(nullif(order_purchase_timestamp,'')) as order_purchase_timestamp_min,
	max(nullif(order_purchase_timestamp,'')) as order_purchase_timestamp_max,
	count(nullif(order_approved_at,'')) as order_approved_at_count,
	min(nullif(order_approved_at,'')) as order_approved_at_min,
	max(nullif(order_approved_at,'')) as order_approved_at_max,
	count(nullif(order_delivered_carrier_date,'')) as order_delivered_carrier_date_count,
	min(nullif(order_delivered_carrier_date,'')) as order_delivered_carrier_date_min,
	max(nullif(order_delivered_carrier_date,'')) as order_delivered_carrier_date_max,
	count(nullif(order_delivered_customer_date,'')) as order_delivered_customer_date_count,
	min(nullif(order_delivered_customer_date,'')) as order_delivered_customer_date_min,
	max(nullif(order_delivered_customer_date,'')) as order_delivered_customer_date_max,
	count(nullif(order_estimated_delivery_date,'')) as order_estimated_delivery_date_count,
	min(nullif(order_estimated_delivery_date,'')) as order_estimated_delivery_date_min,
	max(nullif(order_estimated_delivery_date,'')) as order_estimated_delivery_date_max
from 
	orders
)
select
	total_count, -- 99,440
	order_id_count,
	order_id_dist_count,
	customer_id_count,
	customer_id_dist_count,
	order_status_count,
	order_status_dist_count, -- 8 distinct values
	order_purchase_timestamp_count,
	order_purchase_timestamp_min,
	order_purchase_timestamp_max,
	order_approved_at_count,
	order_approved_at_min,
	order_approved_at_max,
	order_delivered_carrier_date_count,
	order_delivered_carrier_date_min,
	order_delivered_carrier_date_max,
	order_delivered_customer_date_count,
	order_delivered_customer_date_min,
	order_delivered_customer_date_max,
	order_estimated_delivery_date_count,
	order_estimated_delivery_date_min,
	order_estimated_delivery_date_max,
	order_id_count * 1.0 / total_count as perc_fill_order_id, -- 100% filled
	order_id_dist_count * 1.0 / total_count as perc_unique_order_id, -- 100% unique
	customer_id_count * 1.0 / total_count as perc_fill_customer_id, -- 100% filled
	customer_id_dist_count * 1.0 / total_count as perc_unique_customer_id, -- 100% unique >> could benefit being the client_unique_id instead
	order_status_count * 1.0 / total_count as perc_fill_order_status, -- 100% filled
	order_purchase_timestamp_count * 1.0 / total_count as perc_fill_purchase_ts, -- 100% filled
	order_approved_at_count * 1.0 / total_count as perc_fill_approved_ts, -- 99.8% filled
	order_delivered_carrier_date_count * 1.0 / total_count as perc_fill_carrier_date, -- 98.2% filled
	order_delivered_customer_date_count * 1.0 / total_count as perc_fill_delivered_date, -- 97.0% filled
	order_estimated_delivery_date_count * 1.0 / total_count as perc_fill_est_delivery_date -- 100% filled
from
	agg
;

-- Checking values for low-cardinality columns >> order_status
select
	order_status, -- ('approved', 'canceled', 'created', 'delivered', 'invoiced', 'processing', 'shipped', 'unavailable')
	count(1) as cnt
from
	orders
group by
	order_status
order by
	order_status
;

-- Adding table constraints to improve data quality
alter table orders
	add constraint orders_pk primary key ( order_id ),
	alter customer_id set not null,
	alter order_status set not null,
	alter order_purchase_timestamp set not null,
	alter order_estimated_delivery_date set not null,
	add constraint order_status_check check ( order_status in ('approved', 'canceled', 'created', 'delivered', 'invoiced', 'processing', 'shipped', 'unavailable') ),
	add constraint customer_id_unique unique ( customer_id )
;

/*
TABLE PRODUCTS
The relationship cardinality designed in the ERD is not as expected.
It's designed as a 1:1 relationship, meaning that one product can't be sold more than once. This is not true, since we already checked in the order_items
table that customers were buying multiple times the same item in a single cart.
Possibly this cardinality would be better defined by 1:many.

The column product_category_name seems to have low-cardinality, so I'll check it.
It has a mid-size cardinality (73 unique values), I wouldn't recommend adding a constraint in it, because it may require frequent maintenance further.

There's 1 line that doesn't have product_id.
This table is the most problematic of all in this ERD. Some information is missing even from the primary key.
The dimensions of the product have few NULL values, what makes it look like that there was some anomaly to degrade the quality of the data.

Unfortunately, we can't recover the dimensions from the products.
However, we can try to discover which is the missing value for the product_id joining the table with order_items.

Luckily there was only 1 missing product_id in the table order_items, so that should be the match for the table products.

The proper solution would inspect the logs from the transactional system to understand what happened to this table and try to insert the missing values.
Reconstructing the upstream table would be the best solution, but here we'll assume that this missing order_id is the one that we're searching for.

Due to this scenario, I'll only insert the PRIMARY KEY in this table and leave the other columns as they are to avoid polute the table.
In an analytical model, these missing values should be correctly handled, but since this is the source table, I think it's best to leave as close to the
original source as possible.
*/

-- Describing table
with agg as (
select
	count(1) as total_count,
	count(nullif(product_id,'')) as product_id_count,
	count(distinct nullif(product_id,'')) as product_id_dist_count,
	count(nullif(product_category_name,'')) as cat_name_count,
	count(distinct nullif(product_category_name,'')) as cat_name_dist_count,
	count(product_name_lenght) as name_lenght_count,
	count(product_description_lenght) as desc_lenght_count,
	count(product_photos_qty) as photos_qty_count,
	count(product_weight_g) as weight_count,
	count(product_length_cm) as prod_lenght_count,
	count(product_height_cm) as prod_height_count,
	count(product_width_cm) as prod_width_count
from
	products
)
select 
	total_count, -- 32,951
	product_id_count, -- Missing 1 row!
	product_id_dist_count,
	cat_name_count, -- Missing 610 rows!
	cat_name_dist_count, -- 73 distinct values
	name_lenght_count, -- Missing 610 rows!
	desc_lenght_count, -- Missing 610 rows!
	photos_qty_count, -- Missing 610 rows!
	weight_count, -- Missing 2 rows!
	prod_lenght_count, -- Missing 2 rows!
	prod_height_count, -- Missing 2 rows!
	prod_width_count, -- Missing 2 rows!
	product_id_count * 1.0 / total_count as perc_fill_product_id,
	product_id_dist_count * 1.0 / total_count as perc_unique_product_id,
	cat_name_count * 1.0 / total_count as perc_fill_cat_name,
	cat_name_dist_count * 1.0 / total_count as perc_unique_cat_name,
	name_lenght_count * 1.0 / total_count as perc_fill_name_lenght,
	desc_lenght_count * 1.0 / total_count as perc_fill_desc_lenght,
	photos_qty_count * 1.0 / total_count as perc_fill_photos_qty,
	weight_count * 1.0 / total_count as perc_fill_weight,
	prod_lenght_count * 1.0 / total_count as perc_fill_prod_lenght,
	prod_height_count * 1.0 / total_count as perc_fill_prod_height,
	prod_width_count * 1.0 / total_count as perc_fill_prod_width
from
	agg
;

-- Verifying missing values from product_id and product_category_name
select
	*
from 
	products
where
	nullif(product_id, '') is null
	or nullif(product_category_name, '') is null
;

-- Verifying missing values from product_weight_g, product_length_cm and product_height_cm
select
	*
from
	products
where
	product_weight_g is null
	or product_length_cm is null
	or product_height_cm is null
	or product_width_cm is null
;

-- Recovering the order_id from the table products inspecting the order_items >> product_id = 'b5cfb1d3c5e435a7a52227e08f220ee7'
select
	*
from
	order_items as oi
left join
	products as p
		on p.product_id = oi.product_id
where
	nullif(p.product_id, '') is null
;

update products -- Inputing the missing value to match the table order_items >> in a real scenario I'd request it to be inserted in the transactional layer
	set product_id = 'b5cfb1d3c5e435a7a52227e08f220ee7' where nullif(product_id, '') is null
;

-- Adding table constraints to improve data quality
alter table products
	add constraint products_pk primary key ( product_id )
;