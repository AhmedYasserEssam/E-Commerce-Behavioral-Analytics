import json
import os
import re
from datetime import datetime, timezone

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)


HDFS_URI = os.getenv("HDFS_URI", "hdfs://localhost:9000").rstrip("/")

INPUT_PATH = os.getenv(
    "CART_ABANDON_INPUT",
    f"{HDFS_URI}/user/hadoop/ecommerce_input/ecommerce_logs.csv",
)

OUTPUT_PATH = os.getenv(
    "CART_ABANDON_OUTPUT",
    f"{HDFS_URI}/user/hadoop/cart_abandonment_output",
)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "ecommerce_recommendation")
PROFILES_COLLECTION = os.getenv("PROFILES_COLLECTION", "user_profiles")

TOP_N = int(os.getenv("TOP_N", "3"))
GENERATED_AT = datetime.now(timezone.utc).isoformat()


spark = (
    SparkSession.builder
    .appName("CartAbandonmentRecovery")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .getOrCreate()
)

sc = spark.sparkContext


def clean_string(value):
    if value is None:
        return None

    value = str(value).strip()

    if value == "":
        return None

    return value


def extract_category(log):
    for field_name in [
        "category",
        "category_level_1",
        "category_level_2",
        "category_level_3",
        "category_level_4",
        "product_category",
    ]:
        value = clean_string(log.get(field_name))

        if value:
            return value

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

        if match:
            return clean_string(match.group(1))

        return None

    if not isinstance(parsed, dict):
        return None

    for field_name in [
        "category",
        "category_level_1",
        "category_level_2",
        "category_level_3",
        "category_level_4",
        "product_category",
    ]:
        value = clean_string(parsed.get(field_name))

        if value:
            return value

    return None


def event_to_session_record(log):
    session_id = clean_string(log.get("session_id"))
    user_id = clean_string(log.get("user_id"))
    product_id = clean_string(log.get("product_id"))

    event_type = clean_string(log.get("event_type"))

    if event_type:
        event_type = event_type.lower()

    category = extract_category(log)

    return (
        session_id,
        {
            "user_id": user_id,
            "product_id": product_id,
            "event_type": event_type,
            "category": category,
        },
    )


def is_valid_session_event(kv):
    session_id, event = kv

    return (
        session_id is not None
        and event["user_id"] is not None
        and event["product_id"] is not None
        and event["event_type"] is not None
    )


def create_session_accumulator(event):
    is_cart = event["event_type"] == "cart"
    is_purchase = event["event_type"] == "purchase"

    cart_items = []

    if is_cart and event["category"] is not None:
        cart_items.append(
            (
                event["user_id"],
                event["product_id"],
                event["category"],
            )
        )

    return {
        "has_cart": is_cart,
        "has_purchase": is_purchase,
        "cart_items": cart_items,
    }


def merge_session_value(accumulator, event):
    is_cart = event["event_type"] == "cart"
    is_purchase = event["event_type"] == "purchase"

    accumulator["has_cart"] = accumulator["has_cart"] or is_cart
    accumulator["has_purchase"] = accumulator["has_purchase"] or is_purchase

    if is_cart and event["category"] is not None:
        accumulator["cart_items"].append(
            (
                event["user_id"],
                event["product_id"],
                event["category"],
            )
        )

    return accumulator


def merge_session_accumulators(left, right):
    return {
        "has_cart": left["has_cart"] or right["has_cart"],
        "has_purchase": left["has_purchase"] or right["has_purchase"],
        "cart_items": left["cart_items"] + right["cart_items"],
    }


def is_abandoned_session(session_record):
    return (
        session_record["has_cart"]
        and not session_record["has_purchase"]
        and len(session_record["cart_items"]) > 0
    )


def extract_abandoned_cart_items(kv):
    session_id, session_record = kv

    return [
        (
            user_id,
            (
                session_id,
                product_id,
                category,
            ),
        )
        for user_id, product_id, category in session_record["cart_items"]
    ]


def get_nested_value(value, field_name):
    if value is None:
        return None

    if isinstance(value, dict):
        return value.get(field_name)

    try:
        return value[field_name]
    except Exception:
        pass

    return getattr(value, field_name, None)


def top_n_categories(top_categories):
    if not top_categories:
        return []

    cleaned_categories = []

    for category_record in top_categories:
        category = get_nested_value(category_record, "category")
        rank = get_nested_value(category_record, "rank")

        if category is None:
            continue

        try:
            rank = int(rank)
        except (TypeError, ValueError):
            rank = 999999

        cleaned_categories.append(
            {
                "category": str(category),
                "rank": rank,
            }
        )

    cleaned_categories.sort(key=lambda item: (item["rank"], item["category"]))

    return [
        item["category"]
        for item in cleaned_categories[:TOP_N]
    ]


def load_user_top_categories():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

    try:
        collection = client[MONGO_DATABASE][PROFILES_COLLECTION]
        profiles = {}

        for document in collection.find(
            {},
            {
                "_id": 0,
                "user_id": 1,
                "top_categories": 1,
            },
        ):
            user_id = clean_string(document.get("user_id"))

            if user_id is None:
                continue

            profiles[user_id] = top_n_categories(document.get("top_categories"))

        return profiles

    except PyMongoError as exc:
        raise RuntimeError(
            "Could not load MongoDB user profiles. "
            "Phase 3 requires MongoDB user_profiles data."
        ) from exc

    finally:
        client.close()


def flag_row(row):
    user_id, cart_info = row
    session_id, product_id, abandoned_category = cart_info

    top_categories = user_top_categories_broadcast.value.get(user_id, [])

    if top_categories and abandoned_category in top_categories:
        flag = "High_Discount"
    else:
        flag = "Standard_Reminder"

    return {
        "user_id": user_id,
        "session_id": session_id,
        "product_id": product_id,
        "abandoned_category": abandoned_category,
        "user_top_categories": top_categories,
        "flag": flag,
        "generated_at": GENERATED_AT,
    }


output_schema = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("abandoned_category", StringType(), True),
        StructField("user_top_categories", ArrayType(StringType()), True),
        StructField("flag", StringType(), True),
        StructField("generated_at", StringType(), True),
    ]
)


df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("quote", '"')
    .option("escape", '"')
    .csv(INPUT_PATH)
)

required_columns = {
    "session_id",
    "user_id",
    "product_id",
    "event_type",
}

missing_columns = required_columns - set(df.columns)

if missing_columns:
    raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

print("Input columns:")
print(df.columns)

events_rdd = df.rdd.map(lambda row: row.asDict(recursive=True))

session_events = (
    events_rdd
    .map(event_to_session_record)
    .filter(is_valid_session_event)
)

print("Sample session events:")
print(session_events.take(5))

session_summaries = session_events.combineByKey(
    create_session_accumulator,
    merge_session_value,
    merge_session_accumulators,
)

abandoned_sessions = session_summaries.filter(
    lambda kv: is_abandoned_session(kv[1])
)

abandoned_sessions = abandoned_sessions.persist()

abandoned_session_count = abandoned_sessions.count()
print(f"Abandoned sessions: {abandoned_session_count}")

abandoned_cart_items = (
    abandoned_sessions
    .flatMap(extract_abandoned_cart_items)
    .distinct()
    .persist()
)

abandoned_cart_item_count = abandoned_cart_items.count()
print(f"Abandoned cart items: {abandoned_cart_item_count}")

print("Sample abandoned cart items:")
print(abandoned_cart_items.take(5))

profiles_by_user = load_user_top_categories()
print(f"Loaded user profiles from MongoDB: {len(profiles_by_user)}")

print("Sample user top categories:")
print(list(profiles_by_user.items())[:5])

user_top_categories_broadcast = sc.broadcast(profiles_by_user)

flagged_rdd = abandoned_cart_items.map(flag_row).persist()

flag_counts = flagged_rdd.map(
    lambda row: (row["flag"], 1)
).reduceByKey(
    lambda left, right: left + right
)

print("Flag distribution:")
for flag, count in flag_counts.collect():
    print(f"  {flag}: {count}")

if flagged_rdd.isEmpty():
    flagged_df = spark.createDataFrame([], output_schema)
else:
    flagged_df = spark.createDataFrame(flagged_rdd, schema=output_schema)

print("Sample output rows:")
flagged_df.show(10, truncate=False)

(
    flagged_df
    .write
    .mode("overwrite")
    .json(OUTPUT_PATH)
)

print(f"Cart abandonment targeting list written to: {OUTPUT_PATH}")

spark.stop()