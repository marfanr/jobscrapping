import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://superset:"
    + os.environ.get("SUPERSET_DB_PASSWORD", "superset_password")
    + "@superset-db:5432/superset"
)

CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG

WTF_CSRF_ENABLED = True
SUPERSET_WEBSERVER_TIMEOUT = 120