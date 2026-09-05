import dlt
from pyspark.sql import functions as F

# Explicit schema forces Auto Loader to capture these columns immediately
csv_schema = """
    transaction_id STRING,
    customer_id STRING,
    customer_name STRING,
    address STRING,
    product_id STRING,
    category STRING,
    amount DOUBLE,
    timestamp TIMESTAMP
"""


@dlt.table(
    name="bronze_sales",
    comment="Raw sales data ingested continuously via Auto Loader with explicit schema",
)
def bronze_sales():
  return (
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("header", "true")
      .option("schema", csv_schema)
      .option("cloudFiles.cleanSource", "MOVE")
      .option(
          "cloudFiles.cleanSource.moveDestination",
          "/Volumes/workspace/default/raw_archive/",
      )
      .option("cloudFiles.cleanSource.retentionDuration", "0 seconds")
      .load("/Volumes/workspace/default/raw_landing/")
      .withColumn("created_ts", F.current_timestamp())
      .withColumn(
          "source_file",
          F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1),
      )
  )