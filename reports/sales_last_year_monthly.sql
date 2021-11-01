select "year", "month", sum(price) as sales from dwh.fact_order fo 
where fo."timestamp" between (NOW() - INTERVAL '1 YEAR') and now()
group by "year", "month" 
order by 1,2
;