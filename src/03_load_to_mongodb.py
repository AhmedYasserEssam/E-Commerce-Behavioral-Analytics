import ast
import json
import subprocess
from collections import defaultdict
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne


MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ecommerce_recommendation"

HADOOP_CONTAINER = "datanode"

HDFS_MARKET_BASKET_OUTPUT_PATH = "/user/hadoop/market_basket_output/part-*"
HDFS_USER_AFFINITY_OUTPUT_PATH = "/user/hadoop/user_affinity_output/part-*"
HDFS_USER_SOURCE_ITEMS_OUTPUT_PATH = "/user/hadoop/user_source_items_output/part-*"

BATCH_SIZE = 1000
MAX_CO_ITEMS_PER_SOURCE_ITEM = 5


client = MongoClient(MONGO_URI)
db = client[DB_NAME]

user_profiles = db["user_profiles"]
market_basket_pairs = db["market_basket_pairs"]

now = datetime.now(timezone.utc)


def hdfs_lines(hdfs_path):
    command = [
        "docker",
        "exec",
        "-i",
        HADOOP_CONTAINER,
        "hdfs",
        "dfs",
        "-cat",
        hdfs_path
    ]

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8"
    )

    if process.stdout is None:
        raise RuntimeError("Could not read stdout from HDFS command.")

    try:
        for line in process.stdout:
            yield line
    finally:
        return_code = process.wait()

        error_output = ""
        if process.stderr is not None:
            error_output = process.stderr.read()

        if return_code != 0:
            raise RuntimeError(
                f"Failed to read from HDFS path: {hdfs_path}\n"
                f"Command: {' '.join(command)}\n"
                f"Error: {error_output}"
            )


def write_batch(collection, operations):
    if not operations:
        return

    result = collection.bulk_write(operations, ordered=False)

    print(
        collection.name,
        "matched:", result.matched_count,
        "modified:", result.modified_count,
        "inserted:", result.upserted_count
    )

    operations.clear()


def parse_tuple_output_line(line):
    line = line.strip()

    if not line or not line.startswith("("):
        return None

    try:
        return ast.literal_eval(line)
    except (ValueError, SyntaxError):
        return None


def load_user_source_items(hdfs_path):
    user_items_by_user = {}

    for line in hdfs_lines(hdfs_path):
        line = line.strip()

        if not line or not line.startswith("{"):
            continue

        try:
            document = json.loads(line)
        except json.JSONDecodeError:
            continue

        user_id = document.get("user_id")
        source_items = document.get("source_items", [])

        if not user_id:
            continue

        if not isinstance(source_items, list):
            source_items = []

        user_items_by_user[user_id] = source_items

    return user_items_by_user


def build_co_occurrence_recommendations(source_items, co_occurrence_index):
    source_item_set = set(source_items)
    recommendations = []

    for source_item in source_items:
        co_items = []

        for co_item in co_occurrence_index.get(source_item, []):
            if co_item["item"] in source_item_set:
                continue

            co_items.append(co_item)

            if len(co_items) >= MAX_CO_ITEMS_PER_SOURCE_ITEM:
                break

        if co_items:
            recommendations.append({
                "source_item": source_item,
                "co_items": co_items
            })

    return recommendations


def ingest_market_basket_pairs(hdfs_path):
    co_occurrence_index = defaultdict(list)
    market_ops = []

    for line in hdfs_lines(hdfs_path):
        parsed = parse_tuple_output_line(line)

        if parsed is None:
            continue

        pair, count = parsed

        item_a, item_b = sorted(pair)
        count = int(count)

        pair_id = f"{item_a}::{item_b}"

        document = {
            "item_a": item_a,
            "item_b": item_b,
            "items": [item_a, item_b],
            "count": count,
            "updated_at": now
        }

        co_occurrence_index[item_a].append({
            "item": item_b,
            "count": count
        })

        co_occurrence_index[item_b].append({
            "item": item_a,
            "count": count
        })

        market_ops.append(
            UpdateOne(
                {"_id": pair_id},
                {"$set": document},
                upsert=True
            )
        )

        if len(market_ops) >= BATCH_SIZE:
            write_batch(market_basket_pairs, market_ops)

    write_batch(market_basket_pairs, market_ops)

    for co_items in co_occurrence_index.values():
        co_items.sort(key=lambda co_item: (-co_item["count"], co_item["item"]))

    return co_occurrence_index


def ingest_user_profiles(hdfs_path, user_items_by_user, co_occurrence_index):
    profile_ops = []

    for line in hdfs_lines(hdfs_path):
        parsed = parse_tuple_output_line(line)

        if parsed is None:
            continue

        user_id, categories = parsed

        top_categories = []

        for index, category_score in enumerate(categories):
            category, score = category_score

            top_categories.append({
                "category": category,
                "score": int(score),
                "rank": index + 1
            })

        source_items = user_items_by_user.get(user_id, [])

        document = {
            "user_id": user_id,
            "top_categories": top_categories,
            "co_occurrence_recommendations": build_co_occurrence_recommendations(
                source_items,
                co_occurrence_index
            ),
            "updated_at": now
        }

        profile_ops.append(
            UpdateOne(
                {"_id": user_id},
                {"$set": document},
                upsert=True
            )
        )

        if len(profile_ops) >= BATCH_SIZE:
            write_batch(user_profiles, profile_ops)

    write_batch(user_profiles, profile_ops)


def create_indexes():
    user_profiles.create_index("user_id")
    user_profiles.create_index("top_categories.category")
    user_profiles.create_index("co_occurrence_recommendations.source_item")
    user_profiles.create_index("co_occurrence_recommendations.co_items.item")

    market_basket_pairs.create_index("items")
    market_basket_pairs.create_index("count")


try:
    user_items_by_user = load_user_source_items(
        HDFS_USER_SOURCE_ITEMS_OUTPUT_PATH
    )

    co_occurrence_index = ingest_market_basket_pairs(
        HDFS_MARKET_BASKET_OUTPUT_PATH
    )

    ingest_user_profiles(
        HDFS_USER_AFFINITY_OUTPUT_PATH,
        user_items_by_user,
        co_occurrence_index
    )

    create_indexes()

    print("MongoDB ingestion finished.")

finally:
    client.close()