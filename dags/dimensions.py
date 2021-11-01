from datetime import datetime

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag

from utils import SOURCE_SCHEMA, DEST_SCHEMA


@dag(
    default_args={"postgres_conn_id": "dwh_db"},
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    params={"source_schema": SOURCE_SCHEMA, "dest_schema": DEST_SCHEMA},
)
def dimensions():
    path_sql_file_simple_dimensions = "sql/simple_dimensions.sql"

    dest_table = "dim_customer"
    dim_customer = PostgresOperator(
        task_id=dest_table,
        sql=path_sql_file_simple_dimensions,
        params={
            "source_table": "customer",
            "dest_table": dest_table,
            "columns": ["document_number", "full_name", "date_of_birth"],
            "join_column": "document_number",
        },
    )

    dest_table = "dim_company"
    dim_company = PostgresOperator(
        task_id=dest_table,
        sql=path_sql_file_simple_dimensions,
        params={
            "source_table": "company",
            "dest_table": dest_table,
            "columns": ["cuit_number", "name", "is_supplier"],
            "join_column": "cuit_number",
        },
    )

    dest_table = "dim_product"
    dim_product = PostgresOperator(
        task_id=dest_table,
        sql=path_sql_file_simple_dimensions,
        params={
            "source_table": "product",
            "dest_table": dest_table,
            "columns": ["ean"],
            "join_column": "ean",
        },
    )

    dest_table = "dim_company_catalog"
    dim_company_catalog = PostgresOperator(
        task_id=dest_table,
        sql=f"sql/{dest_table}.sql",
        params={
            "source_table": "company_catalog",
            "dest_table": dest_table,
            "columns": ["company_id", "product_id", "price"],
        },
    )

    [dim_customer, dim_company, dim_product] >> dim_company_catalog


dimensions_dag = dimensions()
