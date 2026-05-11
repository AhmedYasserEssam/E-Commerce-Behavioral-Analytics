import json
import os
import re
from datetime import datetime, timezone

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CartAbandonmentRecovery") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017") \
    .config("spark.mongodb.read.database", "ecommerce_recommendation") \
    .getOrCreate()

sc = spark.sparkContext

hdfs_uri = os.getenv("HDFS_URI", "hdfs://localhost:9000").rstrip("/")

input_path = os.getenv(
    "CART_ABANDON_INPUT",
    f"{hdfs_uri}/user/hadoop/ecommerce_input/ecommerce_logs.csv",
)

output_path = os.getenv(
    "CART_ABANDON_OUTPUT",
    f"{hdfs_uri}/user/hadoop/cart_abandonment_output",
)

profiles_collection = "user_profiles"

TOP_N = 3


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

    if isinstance(parsed, dict):
        return parsed.get("category")
    return None


# Read CSV (same options as 02_user_affinity.py)
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(input_path)

# Convert rows to dictionaries for RDD processing
events_rdd = df.rdd.map(lambda row: row.asDict())

# 1) Map each event to (session_id, (user_id, product_id, event_type, category))
session_events = events_rdd.map(
    lambda log: (
        log.get("session_id"),
        (
            log.get("user_id"),
            log.get("product_id"),
            str(log.get("event_type")).lower() if log.get("event_type") else None,
            extract_category(log),
        )
    )
).filter(
    lambda kv: kv[0] not in (None, "")
    and kv[1][0] not in (None, "")
    and kv[1][1] not in (None, "")
    and kv[1][2] not in (None, "")
)

print("Sample session_events:")
print(session_events.take(5))

# 2) Group all events of each session together
session_grouped = session_events.groupByKey().mapValues(list)

# 3) Keep only abandoned sessions:
#    a session that has at least one 'cart' event and zero 'purchase' events
def is_abandoned(events):
    event_types = {e[2] for e in events}
    return "cart" in event_types and "purchase" not in event_types

abandoned_sessions = session_grouped.filter(
    lambda kv: is_abandoned(kv[1])
)

print(f"Abandoned sessions: {abandoned_sessions.count()}")

# 4) From each abandoned session, keep only its cart items
#    Output: (user_id, (session_id, product_id, category))
abandoned_cart_items = abandoned_sessions.flatMap(
    lambda kv: [
        (event[0], (kv[0], event[1], event[3]))
        for event in kv[1]
        if event[2] == "cart" and event[3] not in (None, "")
    ]
).distinct()

print(f"Abandoned cart items: {abandoned_cart_items.count()}")
print("Sample abandoned cart items:")
print(abandoned_cart_items.take(5))

# 5) Load user profiles from MongoDB via the Spark connector
profiles_df = spark.read \
    .format("mongodb") \
    .option("collection", profiles_collection) \
    .load() \
    .select("user_id", "top_categories")

# 6) Convert profiles to an RDD of (user_id, [top N category names])
def top_n_categories(top_categories):
    if not top_categories:
        return []
    sorted_cats = sorted(top_categories, key=lambda c: c["rank"])
    return [c["category"] for c in sorted_cats[:TOP_N]]

user_top_categories = profiles_df.rdd.map(
    lambda row: (row["user_id"], top_n_categories(row["top_categories"]))
)

print("Sample user_top_categories:")
print(user_top_categories.take(5))

# 7) Join abandoned cart items with user top categories
#    Use leftOuterJoin so users without a profile still get a row
joined = abandoned_cart_items.leftOuterJoin(user_top_categories)

# 8) Flag each row:
#    High_Discount     -> abandoned category is in user's top N
#    Standard_Reminder -> otherwise (or user has no profile)
def flag_row(kv):
    user_id, (cart_info, top_cats) = kv
    session_id, product_id, category = cart_info

    if top_cats and category in top_cats:
        flag = "High_Discount"
    else:
        flag = "Standard_Reminder"

    return {
        "user_id": user_id,
        "session_id": session_id,
        "product_id": product_id,
        "abandoned_category": category,
        "user_top_categories": top_cats if top_cats else [],
        "flag": flag,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

flagged_rdd = joined.map(flag_row)

# 9) Print flag distribution
flag_counts = flagged_rdd.map(lambda r: (r["flag"], 1)).reduceByKey(lambda a, b: a + b)
print("Flag distribution:")
for flag, count in flag_counts.collect():
    print(f"  {flag}: {count}")

# 10) Remove any previous output directory before writing
output_hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(output_path)
output_uri = spark._jvm.java.net.URI.create(output_path)
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    output_uri,
    sc._jsc.hadoopConfiguration()
)
if fs.exists(output_hdfs_path):
    fs.delete(output_hdfs_path, True)

# 11) Write output as JSON files to HDFS
flagged_df = spark.createDataFrame(flagged_rdd)

print("Sample output rows:")
flagged_df.show(10, truncate=False)

flagged_df.write.mode("overwrite").json(output_path)

print(f"Cart abandonment targeting list written to: {output_path}")

spark.stop()