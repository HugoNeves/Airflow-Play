from io import StringIO
import csv
import ipaddress
from datetime import datetime

from user_agents import parse

SOURCE_SCHEMA = "b2b_database"
DEST_SCHEMA = "dwh"

FACT_ORDER_SOURCE_COLUMNS_LIST = [
    "customer_id",
    "company_id",
    "supplier_id",
    "product_id",
    "shipping_country_code",
    "price",
    "timestamp",
]
FACT_ORDER_DESTINATION_COLUMNS_LIST = FACT_ORDER_SOURCE_COLUMNS_LIST + [
    "shipping_country",
    "month",
    "year",
]


FACT_WEBLOG_SOURCE_COLUMNS_LIST = [
    "hostname",
    "username",
    "timestamp",
    "user_agent",
]
FACT_WEBLOG_DESTINATION_COLUMNS_LIST = [
    "client_ip",
    "client_country",
    "username",
    "time",
    "user_agent",
    "device",
]


def get_string_buffer(rows_data, fieldnames):
    sio = StringIO()
    writer = csv.DictWriter(sio, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows_data)
    sio.seek(0)
    return sio


def validate_weblog_data(hostname="", timestamp="", user_agent=""):
    try:
        return all(
            [
                ipaddress.ip_address(hostname),
                datetime.strptime(timestamp[1:-1], "%d/%b/%Y:%H:%M:%S %z"),
                parse(user_agent),
            ]
        )
    except (ValueError, TypeError):
        return False
