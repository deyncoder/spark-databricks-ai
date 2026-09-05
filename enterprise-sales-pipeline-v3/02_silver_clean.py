import logging
import dlt
from pyspark.sql import functions as F

# Configure standard Python logging
logger = logging.getLogger("silver_logger")
logging.basicConfig(level=logging.INFO)


# 1. Clean Valid Sales Stream
@dlt.table(
    name="silver_sales",
    comment="Cleaned sales transactions with valid structural rules",
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount >= 0")
def silver_sales():
  logger.info("--- PROCESSING SILVER SALES STREAM ---")
  df = dlt.read_stream("bronze_sales")
  
  # Clean column names natively using Spark transformations
  for col_name in df.columns:
      df = df.withColumnRenamed(col_name, col_name.strip().lower())
      
  logger.info(f"Silver detected columns from Bronze: {df.columns}")
  
  return df.select(
      F.col("transaction_id"),
      F.col("customer_id"),
      F.col("customer_name"),  # Added for SCD2
      F.col("address"),  # Added for SCD2
      F.col("product_id"),
      F.coalesce(F.col("category"), F.lit("Unassigned")).alias(
          "category"
      ),  # Added category fix
      F.col("amount").cast("double"),
      F.col("timestamp").cast("timestamp"),
      F.col("source_file"),
      F.col("created_ts"),
  )


# 2. Quarantine Stream for Bad/Dropped Records
@dlt.table(
    name="silver_quarantine",
    comment="Quarantined records failing data quality expectations",
)
@dlt.expect_or_drop(
    "is_invalid",
    "transaction_id IS NULL OR customer_id IS NULL OR amount < 0",
)
def silver_quarantine():
  df = dlt.read_stream("bronze_sales")
  
  # Clean column names natively using Spark transformations
  for col_name in df.columns:
      df = df.withColumnRenamed(col_name, col_name.strip().lower())
      
  return df.select(
      F.col("transaction_id"),
      F.col("customer_id"),
      F.col("customer_name"),
      F.col("address"),
      F.col("product_id"),
      F.coalesce(F.col("category"), F.lit("Unassigned")).alias("category"),
      F.col("amount"),
      F.col("timestamp"),
      F.col("source_file"),
      F.col("created_ts"),
      F.lit("Failed mandatory structural rules").alias("quarantine_reason"),
  )