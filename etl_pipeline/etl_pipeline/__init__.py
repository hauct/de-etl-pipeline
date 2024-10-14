import os
from dagster import Definitions, load_assets_from_modules

from .jobs import reload_data
from .resources.minio_io_manager import MinIOIOManager
from .resources.mysql_io_manager import MySQLIOManager
from .resources.spark_io_manager import SparkIOManager

from .assets import bronze, silver

bronze_assets = load_assets_from_modules([bronze])
silver_assets = load_assets_from_modules([silver])

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

SPARK_CONFIG = {
    "spark_master": os.getenv("spark://spark-master:7077"),
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
}

resources = {
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    "spark_io_manager": SparkIOManager(SPARK_CONFIG),
}

defs = Definitions(assets=[*bronze_assets, *silver_assets],
                       jobs=[reload_data],
                       resources=resources)