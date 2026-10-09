FROM apache/superset:latest

USER root
RUN uv pip install --no-cache "trino[sqlalchemy]" clickhouse-connect psycopg2-binary
USER superset