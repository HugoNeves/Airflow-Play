insert into {{ params.dest_schema }}.{{ params.dest_table }} 
    ( {% for item in params.columns %}{{ item }}{% if not loop.last %}, {% endif %}{% endfor %} ) 
    select {% for item in params.columns %}t1.{{ item }}{% if not loop.last %}, {% endif %}{% endfor %} 
    from {{ params.source_schema }}.{{ params.source_table }} t1
    left join {{ params.dest_schema }}.{{ params.dest_table }} t2 on t2.{{ params.join_column }} = t1.{{ params.join_column }} 
    where t2.id is null
;