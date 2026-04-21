# Copyright Shashi Preetham Adhimulapu
from pyspark.sql.functions import count, sum

storage_account = "taxidatalake11"
container = "taxi-data"
storage_key = "<YOUR_KEY>" # Please Use Your Key Here....!

try:
    df = spark.read.format("parquet") \
        .option(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key) \
        .load(f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/")

    df_clean = df.dropna()
    df_clean = df_clean.filter(df_clean.trip_distance > 0)

    df_clean.write.format("parquet") \
        .option(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key) \
        .mode("overwrite") \
        .save(f"abfss://{container}@{storage_account}.dfs.core.windows.net/silver/")

    df_gold = df_clean.groupBy("PULocationID").agg(
        count("*").alias("total_trips"),
        sum("total_amount").alias("total_revenue")
    )

    df_gold.write.format("parquet") \
        .option(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key) \
        .mode("overwrite") \
        .save(f"abfss://{container}@{storage_account}.dfs.core.windows.net/gold/")

    display(df_gold)

except Exception as e:
    print("Error:", e)
