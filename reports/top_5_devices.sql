select device, count(*) from dwh.fact_weblog fw 
group by device
order by 2 desc
limit 5
;