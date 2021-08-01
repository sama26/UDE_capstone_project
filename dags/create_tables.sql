CREATE TABLE IF NOT EXISTS public.staging_items (
	test_id varchar(256),
	rfr_id varchar(256),
	rfr_type_code varchar(256),
	location_id varchar(256),
	dangerous_mark varchar(256)
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
	first_use_date varchar(256)
);

CREATE TABLE IF NOT EXISTS public.test_result (
	test_id smallint NOT NULL,
	vehicle_id smallint NOT NULL,
	test_date date,
	test_class_id smallint,
	test_type char(2),
	test_result char(5),
	test_mileage smallint,
	postcode_area char(2),
	CONSTRAINT test_result_pkey PRIMARY KEY (test_id)
);

CREATE TABLE IF NOT EXISTS public.test_item (
	id integer IDENTITY NOT NULL,
	test_id smallint NOT NULL,
	rfr_id smallint,
	rfr_type_code char(1),
	location_id smallint,
	dangerous_mark char(1),
	CONSTRAINT test_item_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.vehicle (
	vehicle_id smallint NOT NULL,
	make char(50),
	model char(50),
	colour char(16),
	fuel_type char(2),
	cylinder_capacity smallint,
	first_use_date date,
	CONSTRAINT vehicle_pkey PRIMARY KEY (vehicle_id)
);
