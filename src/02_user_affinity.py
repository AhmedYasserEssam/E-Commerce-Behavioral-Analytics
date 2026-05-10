import os
import json
import re
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("UserAffinityAggregation") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

sc = spark.sparkContext

hdfs_uri = os.getenv("HDFS_URI", "hdfs://localhost:9000").rstrip("/")

input_path = os.getenv(
    "USER_AFFINITY_INPUT",
    f"{hdfs_uri}/user/hadoop/ecommerce_input/ecommerce_logs.csv",
)

output_path = os.getenv(
    "USER_AFFINITY_OUTPUT",
    f"{hdfs_uri}/user/hadoop/user_affinity_output",
)


def extract_category(log):
    metadata = log.get("product_metadata")
    if metadata in (None, ""):
        return None

    metadata = str(metadata).strip()
    normalized_metadata = metadata.replace('""', '"')
    if normalized_metadata.startswith('"') and normalized_metadata.endswith('"'):
        normalized_metadata = normalized_metadata[1:-1]

    try:
        parsed = json.loads(normalized_metadata)
    except (TypeError, json.JSONDecodeError):
        match = re.search(r'"category"\s*:\s*"([^"]+)"', normalized_metadata)
        return match.group(1) if match else None

    return parsed.get("category")

# Read CSV.
# The metadata columns contain JSON stored inside quoted CSV fields with doubled quotes,
# so Spark needs the CSV escape character set to a double quote as well.
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(input_path)

# Convert rows to dictionaries for RDD processing
events_rdd = df.rdd.map(lambda row: row.asDict())

# Event weights
event_weights = {
    "view": 1,
    "cart": 3,
    "purchase": 5
}

# 1) Keep only valid rows with supported event types and categories
valid_events = events_rdd.map(
    lambda log: (
        log.get("user_id"),
        extract_category(log),
        event_weights.get(str(log.get("event_type")).lower(), 0)
    )
).filter(
    lambda row: (
        row[0] not in (None, "")
        and row[1] not in (None, "")
        and row[2] > 0
    )
)

# 2) Map each event to:
#    ((user_id, category), score)
user_category_scores = valid_events.map(
    lambda row: ((row[0], row[1]), row[2])
)

# 3) Remove rows with score 0
user_category_scores = user_category_scores.filter(
    lambda x: x[1] > 0
)

# 4) Reduce by (user_id, category)
#    Example:
#    (("User_2686", "Electronics"), 1)
#    (("User_2686", "Electronics"), 3)
#    (("User_2686", "Electronics"), 5)
#    becomes:
#    (("User_2686", "Electronics"), 9)
user_category_total_scores = user_category_scores.reduceByKey(
    lambda x, y: x + y
)

# 5) Convert:
#    ((user_id, category), score)
#    into:
#    (user_id, (category, score))
user_category_pairs = user_category_total_scores.map(
    lambda x: (x[0][0], (x[0][1], x[1]))
)

# 6) Group all category scores per user
user_grouped_categories = user_category_pairs.groupByKey().mapValues(list)

# 7) Sort each user's categories by score descending
user_top_categories = user_grouped_categories.mapValues(
    lambda categories: sorted(categories, key=lambda x: x[1], reverse=True)
)

# 8) Remove any previous output directory before writing
output_hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(output_path)
output_uri = spark._jvm.java.net.URI.create(output_path)
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    output_uri,
    sc._jsc.hadoopConfiguration()
)
if fs.exists(output_hdfs_path):
    fs.delete(output_hdfs_path, True)

# 9) Save output
user_top_categories.saveAsTextFile(output_path)

# 10) Print sample results
print("User Affinity Aggregation completed.")
print("Sample results:")

for row in user_top_categories.take(10):
    print(row)

spark.stop()
