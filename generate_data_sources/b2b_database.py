import random
import os

from mimesis import Person, Datetime, Finance, Development, Code, Numeric, Address
from mimesis.enums import EANFormat

from utils import get_database_connection, get_string_buffer, remove_duplicates

person = Person()
datetime = Datetime()
finance = Finance()
development = Development()
code = Code()
numeric = Numeric()
address = Address()

NUMBER_CUSTOMERS = int(os.getenv("NUMBER_CUSTOMERS", "1000"))
NUMBER_COMPANIES = int(os.getenv("NUMBER_COMPANIES", "50"))
NUMBER_PRODUCTS = int(os.getenv("NUMBER_PRODUCTS", "300"))
NUMBER_COMPANY_CATALOG_ITEMS = int(os.getenv("NUMBER_COMPANY_CATALOG_ITEMS", "2500"))
NUMBER_ORDERS = int(os.getenv("NUMBER_ORDERS", "5000"))

## Generate data
rows_customer = [
    {
        "document_number": person.identifier(mask="@@###-###"),
        "full_name": person.full_name(),
        "date_of_birth": datetime.date(start=1950, end=2005),
    }
    for _ in range(NUMBER_CUSTOMERS)
]
rows_company = [
    {
        "cuit_number": person.identifier(mask="@@#####@@"),
        "name": finance.stock_name(),
        "is_supplier": development.boolean(),
    }
    for _ in range(NUMBER_COMPANIES)
]
rows_product = [{"ean": code.ean(fmt=EANFormat.EAN13)} for _ in range(NUMBER_PRODUCTS)]

list_document_number = [customer["document_number"] for customer in rows_customer]
list_cuit_number = [company["cuit_number"] for company in rows_company]
list_cuit_number_companies = [
    company["cuit_number"] for company in rows_company if not company["is_supplier"]
]
list_cuit_number_suppliers = [
    company["cuit_number"] for company in rows_company if company["is_supplier"]
]
list_ean = [product["ean"] for product in rows_product]

rows_company_catalog = [
    {
        "cuit_number": random.choice(list_cuit_number),
        "ean": random.choice(list_ean),
        "price": numeric.float_number(start=0.01, end=100, precision=2),
    }
    for _ in range(NUMBER_COMPANY_CATALOG_ITEMS)
]
rows_company_catalog_clean = list(
    remove_duplicates(rows_company_catalog, lambda d: (d["cuit_number"], d["ean"]))
)

rows_order = [
    {
        "cuit_number_company": random.choice(list_cuit_number_companies),
        "cuit_number_supplier": random.choice(list_cuit_number_suppliers),
        "ean": random.choice(list_ean),
        "document_number": random.choice(list_document_number),
        "shipping_country_code": address.country_code(),
        "price": numeric.float_number(start=0.01, end=100, precision=2),
        "timestamp": datetime.datetime(),
    }
    for _ in range(NUMBER_ORDERS)
]


database_connection = get_database_connection()

## Write to the Database
with database_connection.cursor() as cursor:
    print("Writing to the b2b database...", end=" ")
    cursor.execute("truncate table b2b_database.customer cascade;")
    cursor.copy_expert(
        """
            COPY b2b_database.customer (document_number, full_name, date_of_birth) 
            FROM STDIN WITH CSV HEADER
        """,
        get_string_buffer(
            rows_customer, ["document_number", "full_name", "date_of_birth"]
        ),
    )

    cursor.execute("truncate table b2b_database.company cascade;")
    cursor.copy_expert(
        "COPY b2b_database.company (cuit_number, name, is_supplier) FROM STDIN WITH CSV HEADER",
        get_string_buffer(rows_company, ["cuit_number", "name", "is_supplier"]),
    )

    cursor.execute("truncate table b2b_database.product cascade;")
    cursor.copy_expert(
        "COPY b2b_database.product (ean) FROM STDIN WITH CSV HEADER",
        get_string_buffer(rows_product, ["ean"]),
    )

    cursor.execute("truncate table b2b_database.company_catalog;")
    cursor.copy_expert(
        "COPY b2b_database.company_catalog (cuit_number, ean, price) FROM STDIN WITH CSV HEADER",
        get_string_buffer(rows_company_catalog_clean, ["cuit_number", "ean", "price"]),
    )

    cursor.execute("truncate table b2b_database.order;")
    cursor.copy_expert(
        """
            COPY b2b_database.order (cuit_number_company, cuit_number_supplier, ean, document_number, shipping_country_code, price, timestamp) 
            FROM STDIN WITH CSV HEADER
        """,
        get_string_buffer(
            rows_order,
            [
                "cuit_number_company",
                "cuit_number_supplier",
                "ean",
                "document_number",
                "shipping_country_code",
                "price",
                "timestamp",
            ],
        ),
    )

database_connection.commit()
print("done.")
