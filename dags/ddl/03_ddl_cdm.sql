CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger(
	id	serial PRIMARY KEY,
	courier_id	integer,
	courier_name	varchar,
	settlement_year	integer,
	settlement_month	integer,
	orders_count	integer,
	orders_total_sum	numeric(14, 2),
	rate_avg	NUMERIC,
	order_processing_fee	NUMERIC,
	courier_order_sum	numeric(14, 2),
	courier_tips_sum	numeric(14, 2),
	courier_reward_sum	numeric(14, 2),
	CONSTRAINT courier_unique (courier_id, settlement_year, settlement_month)
);