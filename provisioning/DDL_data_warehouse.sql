DROP SCHEMA IF EXISTS src CASCADE;
CREATE SCHEMA src;

CREATE TABLE src.weblog_data
(
	hostname varchar(15),
	client_identity varchar(1),
	username varchar(50),
	timestamp varchar(50),
	http_request varchar(50),
	status_code int,
	response_size int,
	referer varchar(1000),
	user_agent varchar(200)
); 


DROP SCHEMA IF EXISTS dwh CASCADE;
CREATE SCHEMA dwh;

CREATE TABLE dwh.dim_customer
(
	id serial primary key,
	document_number varchar(50) not null,
	full_name varchar(256),
	date_of_birth date
);

CREATE TABLE dwh.dim_company
(
	id serial primary key,
	cuit_number varchar(50) not null,
	name varchar(256),
	is_supplier boolean
);

CREATE TABLE dwh.dim_product
(
	id serial primary key,
	ean varchar(13) not null
);

CREATE TABLE dwh.dim_company_catalog
(
	id serial primary key,
	company_id int not null,
	product_id int not null,
	price float
);

CREATE TABLE dwh.fact_order
(
	id serial primary key,
	customer_id int not null,
	company_id int not null,
	supplier_id int not null,
	product_id int not null,
	shipping_country_code varchar(2),
	shipping_country varchar(100),
	price float,
	timestamp timestamp,
	month int,
	year int
);

CREATE TABLE dwh.fact_weblog
(
	id serial primary key,
	client_ip varchar(15),
	client_country varchar(100),
	username varchar(50),
	time timestamp,
	user_agent varchar(200),
	device varchar(50)
);
