FROM postgres:14

ARG ddl_file_b2b=DDL_b2b_database.sql
COPY ${ddl_file_b2b} /docker-entrypoint-initdb.d/
RUN chmod +r /docker-entrypoint-initdb.d/${ddl_file_b2b}

ARG ddl_file_dwh=DDL_data_warehouse.sql
COPY ${ddl_file_dwh} /docker-entrypoint-initdb.d/
RUN chmod +r /docker-entrypoint-initdb.d/${ddl_file_dwh}