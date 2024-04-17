FROM python:3.11.5

WORKDIR /app/

COPY ingest.py /app/
COPY dtype_mapping /app/dtype_mapping

RUN    pip install --upgrade pip \
    && pip install pipreqs \
    && pipreqs \
    && pip install -r requirements.txt \
    && pip install psycopg2

ENTRYPOINT [ "python", "ingest.py" ]