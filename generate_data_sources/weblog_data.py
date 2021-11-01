import csv
import os

from mimesis import Internet, Person, Datetime, Numeric
from faker import Faker
from faker.providers import user_agent
from psycopg2.errors import DataError

from utils import get_database_connection

internet = Internet()
person = Person()
datetime = Datetime()
numeric = Numeric()
fake = Faker()
fake.add_provider(user_agent)

NUMBER_LOG_ENTRIES = int(os.getenv("NUMBER_LOG_ENTRIES", "500"))
SOURCE_COLUMNS_LIST = [
    "hostname",
    "client_identity",
    "username",
    "timestamp",
    "http_request",
    "status_code",
    "response_size",
    "referer",
    "user_agent",
]
DESTINATION_COLUMNS_LIST = SOURCE_COLUMNS_LIST

# Generate random data
rows_log = [
    {
        "hostname": internet.ip_v4(),
        "client_identity": "-",
        "username": person.username(),
        "timestamp": f"[{datetime.formatted_datetime(fmt='%d/%b/%Y:%H:%M:%S %z', start=2020, timezone=datetime.timezone())}]",
        "http_request": f"{internet.http_method()} / HTTP/1.0",
        "status_code": internet.http_status_code(),
        "response_size": numeric.integer_number(start=0, end=5000),
        "referer": internet.url(),
        "user_agent": fake.user_agent(),
    }
    for _ in range(NUMBER_LOG_ENTRIES)
]

# Write to file
with open("./access_log", "w", encoding="UTF-8") as file:
    writer = csv.DictWriter(
        file,
        fieldnames=[
            "hostname",
            "client_identity",
            "username",
            "timestamp",
            "http_request",
            "status_code",
            "response_size",
            "referer",
            "user_agent",
        ],
        delimiter=" ",
    )
    writer.writerows(rows_log)


# Read from the file into the src schema in the data warehouse
database_connection = get_database_connection()

with database_connection.cursor() as cursor, open(
    "access_log", encoding="UTF-8"
) as file:
    print("Writing the weblog data to the source schema...", end=" ")
    try:
        cursor.copy_expert(
            """
                COPY src.weblog_data (hostname, client_identity, username, timestamp, http_request, 
                status_code, response_size, referer, user_agent) FROM STDIN WITH CSV DELIMITER ' '
            """,
            file,
        )
    except DataError as exception:
        print(
            f"Error while loading the weblog data from the source file. Details: {exception.pgerror}"
        )

database_connection.commit()
print("done.")
