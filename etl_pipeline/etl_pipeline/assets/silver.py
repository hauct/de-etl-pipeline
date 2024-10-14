from dagster import asset, AssetIn, Output, StaticPartitionsDefinition

import polars as pl
import requests
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *


from pyspark.sql import DataFrame
from datetime import datetime, timedelta
from ..resources.spark_io_manager import get_spark_session


COMPUTE_KIND = "PySpark"
LAYER = "silver"
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1975, datetime.today().year)]
)


# Silver cleaned customer
@asset(
    description="Load 'customers' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    ins={
        "bronze_customer": AssetIn(
            key_prefix=["bronze", "customer"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "customer"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_customer(context, bronze_customer: pl.DataFrame):
    """
    Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
    """

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("Start creating spark session")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        # Convert bronze_book from polars DataFrame to Spark DataFrame
        pandas_df = bronze_customer.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        spark_df = spark_df.dropDuplicates()
        spark_df = spark_df.na.drop()
        # spark_df.cache()
        context.log.info("Got Spark DataFrame")

        # spark_df.unpersist()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_customer",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )