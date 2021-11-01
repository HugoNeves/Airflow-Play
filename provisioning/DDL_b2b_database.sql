DROP SCHEMA IF EXISTS b2b_database CASCADE;
CREATE SCHEMA b2b_database;

CREATE TABLE b2b_database.customer
(
	id serial primary key,
	document_number varchar(50) unique not null,
	full_name varchar(256),
	date_of_birth date
);

CREATE TABLE b2b_database.company
(
	id serial primary key,
	cuit_number varchar(50) unique not null,
	name varchar(256),
	is_supplier boolean
);

CREATE TABLE b2b_database.product
(
	id serial primary key,
	ean varchar(13) unique not null
);

CREATE TABLE b2b_database.company_catalog
(
	cuit_number varchar(50) not null,
	ean varchar(13) not null,
	price float,
	primary key (cuit_number, ean),
	FOREIGN KEY(cuit_number) REFERENCES b2b_database.company(cuit_number),
	FOREIGN KEY(ean) REFERENCES b2b_database.product(ean)
);

CREATE TABLE b2b_database.order
(
	document_number varchar(50) not null,
	cuit_number_company varchar(50) not null,
	cuit_number_supplier varchar(50) not null,
	ean varchar(13) not null,
	shipping_country_code varchar(2),
	price float,
	timestamp timestamp,
    FOREIGN KEY(document_number) REFERENCES b2b_database.customer(document_number),
    FOREIGN KEY(cuit_number_company) REFERENCES b2b_database.company(cuit_number),
	FOREIGN KEY(cuit_number_supplier) REFERENCES b2b_database.company(cuit_number),
	FOREIGN KEY(ean) REFERENCES b2b_database.product(ean)
);
