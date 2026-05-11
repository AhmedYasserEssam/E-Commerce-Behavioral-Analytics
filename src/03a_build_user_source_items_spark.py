import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    collect_list,
    expr,
    lit,
    lower,
    row_number,
    sort_array,
    struct,
    trim,
    when,
)


HDFS_URI = os.getenv("HDFS_URI", "hdfs://localhost:9000").rstrip("/")

INPUT_PATH = os.getenv(
    "RAW_EVENTS_INPUT",
    f"{HDFS_URI}/user/hadoop/ecommerce_input/ecommerce_logs.csv"
)

OUTPUT_PATH = os.getenv(
    "USER_SOURCE_ITEMS_OUTPUT",
    f"{HDFS_URI}/user/hadoop/user_source_items_output"
)

MAX_SOURCE_ITEMS_PER_USER = int(os.getenv("MAX_SOURCE_ITEMS_PER_USER", "20"))


spark = (
    SparkSession.builder
    .appName("BuildUserSourceItems")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .getOrCreate()
)


df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(INPUT_PATH)
)

required_columns = {"user_id", "product_id", "event_type"}
missing_columns = required_columns - set(df.columns)

if missing_columns:
    raise ValueError(f"Missing required columns: {sorted(missing_columns)}")


events = (
    df.select(
        trim(col("user_id")).alias("user_id"),
        trim(col("product_id")).alias("product_id"),
        lower(trim(col("event_type"))).alias("event_type"),
    )
    .where(
        (col("user_id").isNotNull()) &
        (col("user_id") != "") &
        (col("product_id").isNotNull()) &
        (col("product_id") != "")
    )
)

weighted_events = (
    events.withColumn(
        "weight",
        when(col("event_type") == "cart", lit(1))
        .when(col("event_type") == "purchase", lit(2))
        .otherwise(lit(0))
    )
    .where(col("weight") > 0)
)

user_item_scores = (
    weighted_events
    .groupBy("user_id", "product_id")
    .sum("weight")
    .withColumnRenamed("sum(weight)", "score")
)

ranking_window = Window.partitionBy("user_id").orderBy(
    col("score").desc(),
    col("product_id").asc()
)

ranked_items = (
    user_item_scores
    .withColumn("rank", row_number().over(ranking_window))
    .where(col("rank") <= MAX_SOURCE_ITEMS_PER_USER)
)

user_source_items = (
    ranked_items
    .groupBy("user_id")
    .agg(
        sort_array(
            collect_list(
                struct(
                    col("rank"),
                    col("product_id")
                )
            )
        ).alias("ranked_items")
    )
    .select(
        col("user_id"),
        expr("transform(ranked_items, x -> x.product_id)").alias("source_items")
    )
)

(
    user_source_items
    .write
    .mode("overwrite")
    .json(OUTPUT_PATH)
)

print(f"User source items written to: {OUTPUT_PATH}")

spark.stop()