CREATE TABLE IF NOT EXISTS public.staging_items (
	test_id varchar(256),
	rfr_id varchar(256),
	rfr_type_code varchar(256),
	location_id varchar(256),
	dangerous_mark varchar(256),
);

CREATE TABLE IF NOT EXISTS public.staging_results (
	test_id varchar(256),
	vehicle_id varchar(256),
	test_date varchar(256),
	test_class_id varchar(256),
	test_type varchar(256),
	test_result varchar(256),
	test_mileage varchar(256),
	postcode_area varchar(256),
	make varchar(256),
	model varchar(256),
	colour varchar(256),
	fuel_type varchar(256),
	cylinder_capacity varchar(256),
	first_use_date varchar(256),
);

CREATE TABLE IF NOT EXISTS public.test_result (
	test_id integer NOT NULL,
	vehicle_id integer NOT NULL,
	test_date, date,
	test_class_id integer,
	test_type char,
	test_result char,
	test_mileage integer,
	postcode_area char,
	CONSTRAINT test_result_pkey PRIMARY KEY (test_id)
);

CREATE TABLE IF NOT EXISTS public.test_item (
	id integer IDENTITY NOT NULL,
	test_id integer NOT NULL,
	rfr_id integer,
	rfr_type_code char,
	location_id integer,
	dangerous_mark char,
	CONSTRAINT test_item_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.vehicle (
	vehicle_id integer NOT NULL,
	make char,
	model char,
	colour char,
	fuel_type char,
	cylinder_capacity varchar,
	first_use_date datetime,
	CONSTRAINT vehicle_pkey PRIMARY KEY (vehicle_id)
);
