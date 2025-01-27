FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
# COPY ingest_data.py ingest_data.py 
COPY hw_ingest_data.py hw_ingest_data.py

# ENTRYPOINT [ "python", "ingest_data.py" ]
ENTRYPOINT [ "python", "hw_ingest_data.py" ]