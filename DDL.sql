-------------NDS----------------------
drop schema nds cascade;
create schema nds;

drop table nds.invoice ;
create table nds.invoice(
    invoice_id text primary key
      constraint check_invoice_pattern check (invoice_id ~* '^[0-9]{3}-[0-9]{2}-[0-9]{4}$') ,
    branch_id int 
      references nds.branch(branch_id),
    customer_id int not null
      references nds.customer(customer_id),
    product_id int not null 
      references nds.product(product_id),
    quantity int not null,
    date date ,
    time time ,
    payment text 
      constraint check_payment check (payment in ('Cash', 'Credit card', 'Ewallet')),
    rating numeric(3,1) 
       constraint check_rating check (rating <=10) 
);

drop table nds.customer cascade;
create table nds.customer(
    customer_id serial primary key,
    customer_type text constraint check_customer_type check (customer_type in ('Member', 'Normal')) not null,
    gender text constraint check_gender check (gender in ('Male', 'Female')) not null,
    effective_from date not null,
    effective_to date not null
);

drop table nds.branch cascade;
create table nds.branch(
    branch_id serial primary key,
    branch_name text not null,
    branch_city text not null
);


drop table nds.product_line cascade;
create table nds.product_line(
    product_line_id serial primary key,
    product_line_name text not null
);

drop table nds.product cascade;
create table nds.product(
    product_id serial primary key,
    product_line_id int references nds.product_line(product_line_id) not null,
    unit_price numeric not null,
    gross_margin_percentage numeric not null,
    effective_from date not null,
    effective_to date not null
);

-------------DDS----------------------
create schema dds_dim;
create schema dds_fact;

drop table dds_fact.invoice ;
create table dds_fact.invoice(
    invoice_id text primary key,
    branch_id int references dds_dim.branch(branch_id),
    customer_id int references dds_dim.customer(customer_id),
    product_id int references dds_dim.product(product_id),
    date_id int references dds_dim.date(id),
    quantity int,
    tax numeric  ,
    total numeric ,
    time time ,
    payment text,
    unit_price numeric,
    gross_margin_percentage numeric, 
    cogs numeric ,
    gross_income numeric ,
    rating numeric(3,1)
);

drop table dds_dim.customer cascade;
create table dds_dim.customer(
    customer_id int primary key,
    customer_type text,
    gender text,
    customer_category text,
    effective_from date,
    effective_to date
);

drop table dds_dim.branch cascade;
create table dds_dim.branch(
    branch_id int  primary key,
    branch_name char(1),
    branch_city text
);

drop table dds_dim.product cascade;
create table dds_dim.product(
    product_id int  primary key ,
    product_line_name text, 
    effective_from date,
    effective_to date
);

drop table dds_dim.date;
CREATE TABLE dds_dim.date
AS
WITH dates AS (
    SELECT dd::date AS dt
    FROM generate_series
            ('2010-01-01'::timestamp
            , '2030-01-01'::timestamp
            , '1 day'::interval) dd
)
SELECT
    to_char(dt, 'YYYYMMDD')::int AS id ,
    dt AS date,
    to_char(dt, 'YYYY-MM-DD') AS ansi_date,
    date_part('isodow', dt)::int AS day,
    date_part('week', dt)::int AS week_number,
    date_part('month', dt)::int AS month,
    date_part('isoyear', dt)::int AS year,
    (date_part('isodow', dt)::smallint BETWEEN 1 AND 5)::int AS week_day,
    (to_char(dt, 'YYYYMMDD')::int IN (
        20130101,
        20130102,
        20130103,
        20130104,
        20130105,
        20130106,
        20130107,
        20130108,
        20130223,
        20130308,
        20130310,
        20130501,
        20130502,
        20130503,
        20130509,
        20130510,
        20130612,
        20131104,
        20140101,
        20140102,
        20140103,
        20140104,
        20140105,
        20140106,
        20140107,
        20140108,
        20140223,
        20140308,
        20140310,
        20140501,
        20140502,
        20140509,
        20140612,
        20140613,
        20141103,
        20141104,
        20150101,
        20150102,
        20150103,
        20150104,
        20150105,
        20150106,
        20150107,
        20150108,
        20150109,
        20150223,
        20150308,
        20150309,
        20150501,
        20150504,
        20150509,
        20150511,
        20150612,
        20151104,
        20160101,
        20160102,
        20160103,
        20160104,
        20160105,
        20160106,
        20160107,
        20160108,
        20160222,
        20160223,
        20160307,
        20160308,
        20160501,
        20160502,
        20160503,
        20160509,
        20160612,
        20160613,
        20161104,
        20170101,
        20170102,
        20170103,
        20170104,
        20170105,
        20170106,
        20170107,
        20170108,
        20170223,
        20170224,
        20170308,
        20170501,
        20170508,
        20170509,
        20170612,
        20171104,
        20171106,
        20180101,
        20180102,
        20180103,
        20180104,
        20180105,
        20180106,
        20180107,
        20180108,
        20180223,
        20180308,
        20180309,
        20180430,
        20180501,
        20180502,
        20180509,
        20180611,
        20180612,
        20181104,
        20181105,
        20181231,
        20190101,
        20190102,
        20190103,
        20190104,
        20190105,
        20190106,
        20190107,
        20190108,
        20190223,
        20190308,
        20190501,
        20190502,
        20190503,
        20190509,
        20190510,
        20190612,
        20191104,
        20200101, 20200102, 20200103, 20200106, 20200107, 20200108,
       20200224, 20200309, 20200501, 20200504, 20200505, 20200511,
       20200612, 20201104))::int AS holiday
FROM dates
ORDER BY dt;

ALTER TABLE dds_dim.date ADD PRIMARY KEY (id);

------------DATA MART----------------------------
create schema data_mart; 

drop table data_mart.sales;
create table data_mart.sales(
	invoice_id text primary key, 
	branch_name text,
	branch_city text,
	customer_type text, 
	customer_gender text, 
	customer_category text,
	product_line_name text,
	date date, 
	day integer, 
	week_number integer, 
	month integer, 
	year integer, 
	week_day integer, 
	holiday integer,
	quantity integer, 
	tax numeric, 
	total numeric, 
	time time without time zone, 
	payment text, 
	unit_price numeric, 
	gross_margin_percentage numeric, 
	cogs numeric, 
	gross_income numeric, 
	rating numeric)
	
