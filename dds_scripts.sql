create or replace function dds_dim.dim_load()
returns bool
as $$
begin
	insert into dds_dim.customer
	(   customer_id,
	    customer_type,
	    gender,
	    customer_category,
	    effective_from, 
	    effective_to)
	with cte as (
	select customer_id,
		sum(quantity) as ltv
	from nds.invoice
	group by customer_id
	)
	select 
		c.customer_id,
	    c.customer_type,
	    c.gender,
	    case 
	    	when cte.ltv < 50 then 'new'
	    	when cte.ltv between  51 and 100 then 'constant'
	    	else 'gold'
	    end, 
	    c.effective_from, 
	    c.effective_to
	from nds.customer c
	left join cte on cte.customer_id = c.customer_id;


--------------------------------------
	insert into dds_dim.branch 
	select * 
	from nds.branch ;

--------------------------------------

	insert into dds_dim.product 
	select 
		product_id,
		product_line_name,
		effective_from,
		effective_to 
	from nds.product p
	left join nds.product_line pl on p.product_line_id = pl.product_line_id ;

------------------------------------------

	return true;
	exception when others then
		
    	perform public.fn_log('dim_load', '-1. error. ' || sqlerrm, true);
    	return false;
	
end;
$$ language plpgsql;

create or replace function dds_fact.fact_load(
p_initial_start in char(1) default '0', 
p_date_start in date default null,
p_date_end in date default null)
returns bool
as $$
declare 
v_meta_start_date date;
v_meta_end_date date;

begin

	
	if p_initial_start = '1' then          --Берем все данные из nds
		select min("date") from nds.invoice i into v_meta_start_date;
		select max("date") from nds.invoice i into v_meta_end_date;
	
		insert into dds_fact.invoice(
		    invoice_id,
		    branch_id,
		    customer_id,
		    product_id,
		    date_id,
		    quantity,
		    tax,
		    total,
		    time ,
		    payment,
		    unit_price,
		    gross_margin_percentage, 
		    cogs,
		    gross_income ,
		    rating)
		select
			i.invoice_id,
			i.branch_id,
			i.customer_id,
			i.product_id,
			to_char(i."date" , 'YYYYMMDD')::int,
			i.quantity,
			i.quantity * p.unit_price * 0.05,
			(i.quantity * p.unit_price * 0.05) + (i.quantity * p.unit_price),
			i.time,
			i.payment,
			p.unit_price,
			p.gross_margin_percentage,
			(i.quantity * p.unit_price),
			((i.quantity * p.unit_price * 0.05) + (i.quantity * p.unit_price)) * (p.gross_margin_percentage/100),
			i.rating 
		from nds.invoice i 
		left join nds.product p on p.product_id = i.product_id;
		
		

	
	elsif p_date_start is null and p_date_end is null then 
		
		select max(end_date_nds) + interval '1 day' from meta.meta_dds_loads where status = 'ok'  into v_meta_start_date;
		select max("date") from nds.invoice i into v_meta_end_date;
		
		insert into dds_fact.invoice(
		    invoice_id,
		    branch_id,
		    customer_id,
		    product_id,
		    date_id,
		    quantity,
		    tax,
		    total,
		    time ,
		    payment,
		    unit_price,
		    gross_margin_percentage, 
		    cogs,
		    gross_income ,
		    rating)
		select
			i.invoice_id,
			i.branch_id,
			i.customer_id,
			i.product_id,
			to_char(i."date" , 'YYYYMMDD')::int,
			i.quantity,
			i.quantity * p.unit_price * 0.05,
			(i.quantity * p.unit_price * 0.05) + (i.quantity * p.unit_price),
			i.time,
			i.payment,
			p.unit_price,
			p.gross_margin_percentage,
			(i.quantity * p.unit_price),
			((i.quantity * p.unit_price * 0.05) + (i.quantity * p.unit_price)) * (p.gross_margin_percentage/100),
			i.rating
		from nds.invoice i 
		left join nds.product p on p.product_id = i.product_id
		where "date" between v_meta_start_date and v_meta_end_date;		

	
	
	elsif p_date_start is not null and p_date_end is not null then 
		v_meta_start_date := p_date_start;
		v_meta_end_date := p_date_end;
		
		
		MERGE INTO dds_fact.invoice tgt
		USING (select * 
			from nds.invoice i 
			left join nds.product p on p.product_id = i.product_id
			where "date" between v_meta_start_date and v_meta_end_date) src
		ON tgt.inovice_id = src.invoice_id
		WHEN MATCHED THEN
		  UPDATE set tgt.*  = src.*
		WHEN NOT MATCHED THEN
		  INSERT (
		    invoice_id,
		    branch_id,
		    customer_id,
		    product_id,
		    date_id,
		    quantity,
		    tax,
		    total,
		    time ,
		    payment,
		    unit_price,
		    gross_margin_percentage, 
		    cogs,
		    gross_income ,
		    rating)
		  VALUES (src.invoice_id,
			src.branch_id,
			src.customer_id,
			src.product_id,
			to_char(src."date" , 'YYYYMMDD')::int,
			src.quantity,
			src.quantity * src.unit_price * 0.05,
			(src.quantity * src.unit_price * 0.05) + (src.quantity * src.unit_price),
			src.time,
			src.payment,
			src.unit_price,
			src.gross_margin_percentage,
			(src.quantity * src.unit_price),
			((src.quantity * src.unit_price * 0.05) + (src.quantity * src.unit_price)) * (src.gross_margin_percentage/100),
			src.rating);

	end if;
	
	insert into data_mart.sales(
		invoice_id, 
		branch_name, 
		branch_city, 
		customer_type, 
		customer_gender, 
		customer_category, 
		product_line_name,
		date, 
		day, 
		week_number, 
		month, 
		year, 
		week_day, 
		holiday, 
		quantity, 
		tax, 
		total, 
		time, 
		payment, 
		unit_price, 
		gross_margin_percentage, 
		cogs, 
		gross_income, 
		rating)
	select 
		i.invoice_id, 
		br.branch_name,
		br.branch_city,
		c.customer_type ,
		c.gender ,
		c.customer_category, 
		p.product_line_name,
		d.date,
		d."day" ,
		d.week_number ,
		d."month" ,
		d."year" ,
		d.week_day ,
		d.holiday ,
		i.quantity, 
		i.tax, 
		i.total, 
		i.time, 
		i.payment, 
		i.unit_price, 
		i.gross_margin_percentage, 
		i.cogs, 
		i.gross_income, 
		i.rating
	from dds_fact.invoice i
	left join dds_dim.branch br on br.branch_id = i.branch_id 
	left join dds_dim.customer c on c.customer_id = i.customer_id 
	left join dds_dim.date d on d.id = i.date_id 
	left join dds_dim.product p on p.product_id = i.product_id 	
	where d."date" between v_meta_start_date and v_meta_end_date
	ON CONFLICT (invoice_id)
	DO UPDATE SET
    branch_name = excluded.branch_name,
    branch_city = excluded.branch_city,
    customer_type = excluded.customer_type,
    customer_gender = excluded.customer_gender,
    customer_category = excluded.customer_category,
    product_line_name = excluded.product_line_name,
    "date" = excluded."date",
    day = excluded.day,
    week_number = excluded.week_number,
    month = excluded.month,
    year = excluded.year,
    week_day = excluded.week_day,
    holiday = excluded.holiday,
    quantity = excluded.quantity,
    tax = excluded.tax,
    total = excluded.total,
    "time" = excluded."time",
    payment = excluded.payment,
    unit_price = excluded.unit_price,
    gross_margin_percentage = excluded.gross_margin_percentage,
    cogs = excluded.cogs,
    gross_income = excluded.gross_income,
    rating = excluded.rating;
	
	insert into meta.meta_dds_loads(load_date, start_date_nds, end_date_nds, status)
	values (now()::date, v_meta_start_date, v_meta_end_date, 'ok');
	
	return true;
	exception when others then
    	insert into meta.meta_dds_loads(load_date, start_date_nds, end_date_nds, status)
			values (now()::date, v_meta_start_date, v_meta_end_date, sqlerrm);		
		
    	perform public.fn_log('fact_load', '-1. error. ' || sqlerrm, true);
    	return false;
	
end;
$$ language plpgsql;

select dds_fact.fact_load(p_initial_start := '1'); 