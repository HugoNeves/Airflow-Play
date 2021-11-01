from datetime import datetime

from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.decorators import task, dag
from airflow.models import Variable

from utils import (
    get_string_buffer,
    validate_weblog_data,
    SOURCE_SCHEMA,
    DEST_SCHEMA,
    FACT_ORDER_SOURCE_COLUMNS_LIST,
    FACT_ORDER_DESTINATION_COLUMNS_LIST,
    FACT_WEBLOG_SOURCE_COLUMNS_LIST,
    FACT_WEBLOG_DESTINATION_COLUMNS_LIST,
)


@dag(schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False)
def facts():

    postgres_hook = PostgresHook(postgres_conn_id="dwh_db")
    database_connection = postgres_hook.get_conn()

    @task
    def fact_order():
        from pytz import country_names

        dest_table = "fact_order"

        # Extract
        with database_connection.cursor() as cursor:
            cursor.execute(
                f"""
                    select 
                        dc.id as customer_id, 
                        dc2.id as company_id, 
                        dc3.id as supplier_id, 
                        dp.id as product_id, 
                        o.shipping_country_code, 
                        o.price, 
                        o."timestamp" 
                    from {SOURCE_SCHEMA}.order o 
                    join {DEST_SCHEMA}.dim_customer dc on dc.document_number = o.document_number 
                    join {DEST_SCHEMA}.dim_company dc2 on dc2.cuit_number = o.cuit_number_company 
                    join {DEST_SCHEMA}.dim_company dc3 on dc3.cuit_number = o.cuit_number_supplier 
                    join {DEST_SCHEMA}.dim_product dp on dp.ean = o.ean 
                    left join {DEST_SCHEMA}.{dest_table} fo on fo.customer_id = dc.id and 
                                                    fo.company_id = dc2.id and 
                                                    fo.product_id = dp.id and 
                                                    fo."timestamp" = o."timestamp" 
                    where fo.company_id is null
                    ;
                """
            )

            list_of_dict = [
                dict(zip(FACT_ORDER_SOURCE_COLUMNS_LIST, values))
                for values in cursor.fetchall()
            ]

            # Transform
            for row in list_of_dict:
                timestamp = row["timestamp"]
                row["month"] = timestamp.month
                row["year"] = timestamp.year
                row["shipping_country"] = country_names.get(
                    row["shipping_country_code"], "N/A"
                )

            # Load
            cursor.copy_expert(
                f"""
                    COPY {DEST_SCHEMA}.{dest_table}
                    ({", ".join(FACT_ORDER_DESTINATION_COLUMNS_LIST)}) 
                    FROM STDIN WITH CSV HEADER
                    ;
                """,
                get_string_buffer(
                    list_of_dict,
                    FACT_ORDER_DESTINATION_COLUMNS_LIST,
                ),
            )
        database_connection.commit()

    @task
    def fact_weblog():
        from user_agents import parse
        import ipinfo

        ip_info_access_token = Variable.get("IP_INFO_ACCESS_TOKEN")

        dest_table = "fact_weblog"

        def get_ip_country_mapping(list_hostname):
            handler = ipinfo.getHandler(ip_info_access_token)
            hostnames_details = handler.getBatchDetails(list_hostname)

            return hostnames_details

        # Extract
        with database_connection.cursor() as cursor:
            cursor.execute(
                f"""
                    select {", ".join(FACT_WEBLOG_SOURCE_COLUMNS_LIST)} FROM src.weblog_data
                    ;
                """
            )

            list_of_dict = [
                dict(zip(FACT_WEBLOG_SOURCE_COLUMNS_LIST, values))
                for values in cursor.fetchall()
            ]

            # Transform
            list_hostnames = []
            weblog_data_transformed = []
            for row in list_of_dict:
                hostname = row.get("hostname", "")
                timestamp = row.get("timestamp", "")
                user_agent = row.get("user_agent", "")
                if validate_weblog_data(hostname, timestamp, user_agent):
                    list_hostnames.append(hostname)
                    device = parse(user_agent).device.family
                    weblog_data_transformed.append(
                        {
                            "client_ip": hostname,
                            "client_country": "",
                            "username": row.get("username", "N/A"),
                            "time": timestamp,
                            "user_agent": user_agent,
                            "device": device,
                        }
                    )
                else:
                    print(
                        f"One of hostname, timestamp or user_agent are not valid. Hostname: {hostname}, Timestamp: {timestamp}, User Agent: {user_agent}"
                    )

            ip_country_dict = get_ip_country_mapping(set(list_hostnames))

            for row in weblog_data_transformed:
                hostname = row["client_ip"]
                row["client_country"] = (
                    ip_country_dict[hostname].get("country_name", "N/A")
                    if hostname in ip_country_dict
                    else "N/A"
                )

            # Load
            with database_connection.cursor() as cursor:
                cursor.execute(f"""TRUNCATE TABLE {DEST_SCHEMA}.{dest_table};""")
                cursor.copy_expert(
                    f"""
                        COPY {DEST_SCHEMA}.{dest_table} ({", ".join(FACT_WEBLOG_DESTINATION_COLUMNS_LIST)}) 
                        FROM STDIN WITH CSV HEADER
                        ;
                    """,
                    get_string_buffer(
                        weblog_data_transformed, FACT_WEBLOG_DESTINATION_COLUMNS_LIST
                    ),
                )
        database_connection.commit()

    [fact_order(), fact_weblog()]


facts_dag = facts()
