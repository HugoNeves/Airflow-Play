
FROM python:3.10-slim

# Install pre-requirements for Psycopg 2
RUN apt update \
    && apt -y install libpq-dev gcc

# Install Pipenv
RUN python3.10 -m pip install pipenv

# Install requirements
COPY Pipfile .
COPY Pipfile.lock .
RUN pipenv lock -r > requirements.txt
RUN python3.10 -m pip install -r requirements.txt

ENV PYTHONPATH .

# Copy the code to the image
COPY utils.py .

ENV file_name_b2b_database=b2b_database.py
ENV file_name_weblog_data=weblog_data.py

COPY ${file_name_weblog_data} ${file_name_b2b_database} ./
RUN /bin/chmod +x ${file_name_weblog_data}
RUN /bin/chmod +x ${file_name_b2b_database}