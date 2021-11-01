with country as 
(
	select client_country, count(*) from dwh.fact_weblog fw 
	group by client_country 
	order by 2 desc
	limit 1
)
select dp.ean, count(*) from dwh.fact_order fo 
join dwh.dim_product dp on dp.id = fo.product_id 
join country c on c.client_country = fo.shipping_country 
group by dp.ean 
order by 2 desc
limit 10
;