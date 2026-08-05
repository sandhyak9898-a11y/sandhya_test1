from pyspark.sql import functions as F

source_table = "tv_model_sales_raw"
target_table = "tv_model_sales_curated"

target_abfss_path = "abfss://container@storageaccount.dfs.core.windows.net/sales/tv_model_sales_curated"

df = spark.read.format("delta").table(source_table) \
    .select(
        "tv_model_id",
        "tv_model_name",
        "region",
        "units_sold",
        "revenue_amount",
        "service_year"
    )

df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("service_year") \
    .option("path", target_abfss_path) \
    .saveAsTable(target_table)