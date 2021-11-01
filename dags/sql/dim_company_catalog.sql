insert into {{ params.dest_schema }}.{{ params.dest_table }} 
    ( {% for item in params.columns %}{{ item }}{% if not loop.last %}, {% endif %}{% endfor %} )
    select dc.id as company_id, dp.id as product_id, cc.price from {{ params.source_schema }}.{{ params.source_table }} cc 
    join {{ params.dest_schema }}.dim_company dc on dc.cuit_number = cc.cuit_number 
    join {{ params.dest_schema }}.dim_product dp on dp.ean = cc.ean 
    left join {{ params.dest_schema }}.{{ params.dest_table }} dcc on dcc.company_id = dc.id and dcc.product_id = dp.id
    where dcc.company_id is null
;