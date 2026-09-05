import dlt
from pyspark.sql import functions as F


# 1. Date Dimension Table
@dlt.table(
    name="gold_dim_date",
    comment="Gold Date Dimension table derived from sales transactions",
)
@dlt.expect_or_drop("valid_date", "date_key IS NOT NULL")
def gold_dim_date():
  sales_df = dlt.read("silver_sales")

  dates_df = sales_df.select(
      F.to_date(F.col("timestamp")).alias("date_key")
  ).distinct()

  return dates_df.select(
      F.col("date_key"),
      F.year("date_key").alias("year"),
      F.quarter("date_key").alias("quarter"),
      F.month("date_key").alias("month"),
      F.dayofmonth("date_key").alias("day"),
      F.date_format("date_key", "EEEE").alias("day_of_week"),
      F.when(F.dayofweek("date_key").isin([1, 7]), True)
      .otherwise(False)
      .alias("is_weekend"),
  )


# 2. Customer Dimension (SCD Type 2 setup)
dlt.create_streaming_table(
    name="gold_dim_customer",
    comment="SCD Type 2 Customer Dimension tracking history",
)

dlt.apply_changes(
    target="gold_dim_customer",
    source="silver_sales",
    keys=["customer_id"],
    sequence_by=F.col("timestamp"),
    stored_as_scd_type="2",
    track_history_column_list=["customer_name", "address"],
    except_column_list=["transaction_id", "product_id", "category", "amount", "source_file", "created_ts"],
)


# 3. Product Dimension (With Category Attribute)
@dlt.table(
    name="gold_dim_product",
    comment="Gold Product Dimension tracking unique product profiles and categories",
)
def gold_dim_product():
  return (
      dlt.read("silver_sales")
      .select("product_id", "category")
      .distinct()
      .withColumn("updated_at", F.current_timestamp())
  )


# 4. Fact Sales Table linked to all Dimensions & Date Key
@dlt.table(
    name="gold_fact_sales",
    comment="Gold Fact Sales table linking transactions to dimensions and date",
)
def gold_fact_sales():
  sales_df = dlt.read("silver_sales")

  return sales_df.select(
      F.col("transaction_id"),
      F.col("customer_id"),
      F.col("product_id"),
      F.to_date(F.col("timestamp")).alias("date_key"),
      F.col("amount"),
      F.col("timestamp"),
  )