CREATE TABLE IF NOT EXISTS stg.couriers(
	id serial PRIMARY KEY,
	courier_id varchar(30),
	name varchar 
);

CREATE TABLE IF NOT EXISTS stg.deliveries(
	id serial PRIMARY KEY,
	order_id varchar(30),
	order_ts timestamp,
	delivery_id varchar(30),
	courier_id 	varchar(30),
	address varchar,
	delivery_ts timestamp,
	rate integer,
	sum numeric (14, 2),
	tip_sum numeric (14, 2)
);

ALTER TABLE stg.couriers ADD CONSTRAINT couriers_un UNIQUE (_id);

ALTER TABLE stg.deliveries ADD CONSTRAINT deliveries_un UNIQUE (delivery_id);
