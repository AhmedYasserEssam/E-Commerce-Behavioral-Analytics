import argparse
import json
import os
import time
from datetime import date, datetime

from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "ecommerce_recommendation")
PROFILES_COLLECTION = os.getenv("PROFILES_COLLECTION", "user_profiles")
MARKET_BASKET_COLLECTION = os.getenv(
    "MARKET_BASKET_COLLECTION",
    "market_basket_pairs"
)
SERVER_SELECTION_TIMEOUT_MS = int(os.getenv("MONGO_TIMEOUT_MS", "5000"))
DEFAULT_LIMIT = int(os.getenv("QUERY_DEMO_LIMIT", "5"))


def json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    return str(value)


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Fetch recommendation data for a user_id and item_id from MongoDB. "
            "The script first checks the denormalized user_profiles document, "
            "then falls back to the global market_basket_pairs collection if "
            "the item is not embedded for that user."
        )
    )

    parser.add_argument("user_id", help="User ID, for example User_12762")
    parser.add_argument("item_id", help="Source item ID, for example ITEM_4482")

    parser.add_argument(
        "--mongo-uri",
        default=MONGO_URI,
        help=f"MongoDB URI. Defaults to {MONGO_URI}",
    )
    parser.add_argument(
        "--database",
        default=MONGO_DATABASE,
        help=f"MongoDB database name. Defaults to {MONGO_DATABASE}",
    )
    parser.add_argument(
        "--collection",
        default=PROFILES_COLLECTION,
        help=f"User profile collection. Defaults to {PROFILES_COLLECTION}",
    )
    parser.add_argument(
        "--market-basket-collection",
        default=MARKET_BASKET_COLLECTION,
        help=(
            "Global market basket collection used as a fallback. "
            f"Defaults to {MARKET_BASKET_COLLECTION}"
        ),
    )
    parser.add_argument(
        "--limit",
        default=DEFAULT_LIMIT,
        type=int,
        help=f"Maximum number of recommendations to show. Defaults to {DEFAULT_LIMIT}",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the lookup result as JSON.",
    )

    args = parser.parse_args()

    if args.limit < 1:
        raise SystemExit("--limit must be at least 1")

    return args


def compact_top_categories(top_categories):
    categories = []

    for category_record in top_categories or []:
        if not isinstance(category_record, dict):
            continue

        categories.append(
            {
                "rank": category_record.get("rank"),
                "category": category_record.get("category"),
                "score": category_record.get("score"),
            }
        )

    categories.sort(
        key=lambda category: (
            category.get("rank") if category.get("rank") is not None else 999999,
            str(category.get("category") or ""),
        )
    )

    return categories


def find_embedded_recommendation_block(profile, item_id):
    recommendation_blocks = profile.get("co_occurrence_recommendations", []) or []

    for recommendation_block in recommendation_blocks:
        if not isinstance(recommendation_block, dict):
            continue

        if recommendation_block.get("source_item") == item_id:
            return recommendation_block

    return None


def source_items_from_profile(profile):
    source_items = []

    for recommendation_block in profile.get("co_occurrence_recommendations", []) or []:
        if not isinstance(recommendation_block, dict):
            continue

        source_item = recommendation_block.get("source_item")

        if source_item:
            source_items.append(source_item)

    return source_items


def clean_recommendations(recommendations, limit):
    cleaned = []

    for recommendation in recommendations or []:
        if not isinstance(recommendation, dict):
            continue

        item = recommendation.get("item")

        if not item:
            continue

        cleaned.append(
            {
                "item": item,
                "count": recommendation.get("count"),
            }
        )

    cleaned.sort(
        key=lambda recommendation: (
            -int(recommendation.get("count") or 0),
            str(recommendation.get("item") or ""),
        )
    )

    return cleaned[:limit]


def lookup_global_item_recommendations(collection, item_id, limit):
    started_at = time.perf_counter()

    cursor = (
        collection
        .find(
            {"items": item_id},
            {
                "_id": 0,
                "item_a": 1,
                "item_b": 1,
                "count": 1,
            },
        )
        .sort("count", -1)
        .limit(limit)
    )

    pairs = list(cursor)
    lookup_ms = (time.perf_counter() - started_at) * 1000

    recommendations = []

    for pair in pairs:
        item_a = pair.get("item_a")
        item_b = pair.get("item_b")

        if item_a == item_id:
            recommended_item = item_b
        elif item_b == item_id:
            recommended_item = item_a
        else:
            continue

        recommendations.append(
            {
                "item": recommended_item,
                "count": pair.get("count"),
            }
        )

    return clean_recommendations(recommendations, limit), round(lookup_ms, 3)


def lookup_recommendations(
    profiles_collection,
    market_basket_collection,
    user_id,
    item_id,
    limit,
):
    query = {
        "$or": [
            {"_id": user_id},
            {"user_id": user_id},
        ]
    }

    projection = {
        "_id": 0,
        "user_id": 1,
        "top_categories": 1,
        "updated_at": 1,
        "co_occurrence_recommendations": 1,
    }

    started_at = time.perf_counter()
    profile = profiles_collection.find_one(query, projection)
    profile_lookup_ms = (time.perf_counter() - started_at) * 1000

    if profile is None:
        return {
            "found_user": False,
            "found_embedded_source_item": False,
            "found_item_recommendations": False,
            "used_global_fallback": False,
            "user_id": user_id,
            "item_id": item_id,
            "profile_lookup_ms": round(profile_lookup_ms, 3),
            "global_lookup_ms": 0,
            "total_lookup_ms": round(profile_lookup_ms, 3),
            "top_categories": [],
            "recommendations": [],
            "available_source_items": [],
            "profile_updated_at": None,
        }

    available_source_items = source_items_from_profile(profile)
    recommendation_block = find_embedded_recommendation_block(profile, item_id)

    global_lookup_ms = 0
    used_global_fallback = False

    if recommendation_block is not None:
        recommendations = clean_recommendations(
            recommendation_block.get("co_items", []),
            limit,
        )
    else:
        used_global_fallback = True
        recommendations, global_lookup_ms = lookup_global_item_recommendations(
            market_basket_collection,
            item_id,
            limit,
        )

    return {
        "found_user": True,
        "found_embedded_source_item": recommendation_block is not None,
        "found_item_recommendations": bool(recommendations),
        "used_global_fallback": used_global_fallback,
        "user_id": profile.get("user_id", user_id),
        "item_id": item_id,
        "profile_lookup_ms": round(profile_lookup_ms, 3),
        "global_lookup_ms": global_lookup_ms,
        "total_lookup_ms": round(profile_lookup_ms + global_lookup_ms, 3),
        "top_categories": compact_top_categories(profile.get("top_categories")),
        "recommendations": recommendations,
        "available_source_items": available_source_items,
        "profile_updated_at": profile.get("updated_at"),
    }


def print_human_result(result, database, profiles_collection, market_basket_collection):
    print("Instant recommendation lookup")
    print(f"User profile collection: {database}.{profiles_collection}")
    print(f"Market basket collection: {database}.{market_basket_collection}")
    print("Primary lookup: denormalized user_profiles document query")
    print("Fallback: global market_basket_pairs query if item is not embedded")
    print(f"Profile lookup time: {result['profile_lookup_ms']} ms")

    if result.get("used_global_fallback"):
        print(f"Global fallback lookup time: {result['global_lookup_ms']} ms")

    print(f"Total lookup time: {result['total_lookup_ms']} ms")
    print()

    if not result["found_user"]:
        print(f"No user profile found for user_id={result['user_id']}")
        return

    print(f"User: {result['user_id']}")
    print(f"Source item: {result['item_id']}")

    if result.get("profile_updated_at"):
        print(f"Profile updated at: {result['profile_updated_at']}")

    print()
    print("Top categories:")

    if result["top_categories"]:
        for category in result["top_categories"]:
            rank = category.get("rank", "?")
            name = category.get("category", "Unknown")
            score = category.get("score", "?")
            print(f"  {rank}. {name} (score: {score})")
    else:
        print("  No top categories stored for this user.")

    print()

    if result.get("found_embedded_source_item"):
        print("Recommendation source: embedded user profile")
    else:
        print(
            "Recommendation source: global market basket fallback "
            "(the item is not embedded as a source item for this user)"
        )

        if result["available_source_items"]:
            print(
                "Embedded source items available for this user: "
                + ", ".join(result["available_source_items"])
            )

    print()

    if not result["recommendations"]:
        print(
            "No co-occurrence recommendations found for "
            f"item_id={result['item_id']}."
        )
        return

    print("Recommended co-occurring items:")

    for index, recommendation in enumerate(result["recommendations"], start=1):
        item = recommendation.get("item", "Unknown")
        count = recommendation.get("count", "?")
        print(f"  {index}. {item} (co-purchase count: {count})")


def main():
    args = parse_args()

    client = MongoClient(
        args.mongo_uri,
        serverSelectionTimeoutMS=SERVER_SELECTION_TIMEOUT_MS,
    )

    try:
        db = client[args.database]
        profiles_collection = db[args.collection]
        market_basket_collection = db[args.market_basket_collection]

        result = lookup_recommendations(
            profiles_collection,
            market_basket_collection,
            args.user_id,
            args.item_id,
            args.limit,
        )

        if args.json:
            print(json.dumps(result, indent=2, default=json_default))
        else:
            print_human_result(
                result,
                args.database,
                args.collection,
                args.market_basket_collection,
            )

    except ServerSelectionTimeoutError as exc:
        raise SystemExit(
            "Could not connect to MongoDB. Make sure the local MongoDB "
            f"container is running and reachable at {args.mongo_uri}.\n{exc}"
        )
    except PyMongoError as exc:
        raise SystemExit(f"MongoDB query failed: {exc}")
    finally:
        client.close()


if __name__ == "__main__":
    main()