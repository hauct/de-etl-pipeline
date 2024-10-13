import os
from dagster import Definitions, load_assets_from_modules

from .jobs import reload_data
from .resources.minio_io_manager import MinIOIOManager
from .resources.mysql_io_manager import MySQLIOManager

from .assets import bronze

MYSQL_CONFIG = {
    "host": 'localhost',
    "port": '3307',
    "database": 'olist',
    "user": 'admin',
    "password": 'admin'
}

MINIO_CONFIG = {
    "endpoint_url": 'minio:9000',
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
}

# MYSQL_CONFIG = {
#     "host": os.getenv("MYSQL_HOST"),
#     "port": os.getenv("MYSQL_PORT"),
#     "database": os.getenv("MYSQL_DATABASES"),
#     "user": os.getenv("MYSQL_ROOT_USER"),
#     "password": os.getenv("MYSQL_ROOT_PASSWORD"),
# }

# MINIO_CONFIG = {
#     "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#     "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#     "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     "bucket": os.getenv("DATALAKE_BUCKET"),
# }

resources = {
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
}

defs = Definitions(assets=load_assets_from_modules([bronze]),
                       jobs=[reload_data],
                       resources=resources)