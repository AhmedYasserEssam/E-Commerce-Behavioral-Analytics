# E-Commerce Behavioral Analytics

This project analyzes e-commerce clickstream behavior with a local Hadoop/Spark workflow and stores recommendation-ready results in MongoDB.

The main work completed so far is:

- Set up a local Hadoop Docker cluster.
- Loaded the raw e-commerce log dataset into HDFS.
- Built a Spark market basket analysis job.
- Built a Spark user affinity aggregation job.
- Built a Spark user source-items job for denormalized recommendations.
- Stored Spark outputs in HDFS for ingestion.
- Added MongoDB with Docker Compose.
- Loaded the processed analytics output into MongoDB collections.

## Project Structure

```text
.
|-- data/raw/ecommerce_logs.csv
|-- docker-compose.mongo.yml
|-- docker-hadoop/
|   |-- docker-compose.yml
|   |-- hadoop.env
|   |-- market_basket_output.txt
|   `-- user_affinity_output.txt
|-- src/
|   |-- 01_market_basket.py
|   |-- 02_user_affinity.py
|   |-- 03a_build_user_source_items_spark.py
|   |-- 03_load_to_mongodb.py
|   `-- 04_cart_abandonment.py
|-- HADOOP_HDFS_DATA_LOADING_GUIDE.md
`-- requirements.txt
```

## Dataset

The input dataset is stored at:

```text
data/raw/ecommerce_logs.csv
```

It contains e-commerce behavior events with these columns:

```text
timestamp, session_id, user_id, event_type, product_id, price, referrer, user_metadata, product_metadata
```

The important fields used by the current analytics jobs are:

- `session_id`: groups activity into shopping sessions.
- `user_id`: identifies the user for personalization.
- `event_type`: includes events such as `view`, `cart`, and `purchase`.
- `product_id`: identifies products for market basket analysis.
- `product_metadata`: contains product details such as category.

## Hadoop and HDFS Setup

The Hadoop cluster is managed from the `docker-hadoop` directory.

The dataset was loaded from local disk into HDFS at:

```text
/user/hadoop/ecommerce_input/ecommerce_logs.csv
```

Spark reads the file through:

```text
hdfs://localhost:9000/user/hadoop/ecommerce_input/ecommerce_logs.csv
```

The detailed HDFS setup and troubleshooting notes are documented in:

```text
HADOOP_HDFS_DATA_LOADING_GUIDE.md
```

Important Hadoop/Spark configuration work completed:

- Exposed the NameNode web UI on `localhost:9870`.
- Used HDFS RPC on `localhost:9000` for Spark reads.
- Exposed DataNode ports needed by Spark running outside Docker.
- Set `spark.hadoop.dfs.client.use.datanode.hostname=true` in the Spark jobs.
- Adjusted Hadoop configuration so DataNode access works from the host machine.

## Spark Job 1: Market Basket Analysis

File:

```text
src/01_market_basket.py
```

Purpose:

Find products that are purchased together in the same session.

What the job does:

1. Reads `ecommerce_logs.csv` from HDFS.
2. Keeps only rows where `event_type` is `purchase`.
3. Maps each purchase to `(session_id, product_id)`.
4. Groups unique purchased products by session.
5. Generates unordered product pairs per session.
6. Counts how often each product pair appears globally.
7. Sorts product pairs by frequency.
8. Saves the output to HDFS.

HDFS output path:

```text
hdfs://localhost:9000/user/hadoop/market_basket_output
```

Optional local exported output:

```text
docker-hadoop/market_basket_output.txt
```

Example output format:

```text
(('ITEM_1116', 'ITEM_4826'), 2)
```

This means `ITEM_1116` and `ITEM_4826` were purchased together in two sessions.

## Spark Job 2: User Affinity Aggregation

File:

```text
src/02_user_affinity.py
```

Purpose:

Create user preference profiles by scoring each user's interaction with product categories.

What the job does:

1. Reads `ecommerce_logs.csv` from HDFS.
2. Parses the JSON-like `product_metadata` field.
3. Extracts each product's category.
4. Scores events with these weights:

```text
view = 1
cart = 3
purchase = 5
```

5. Aggregates scores by `(user_id, category)`.
6. Groups category scores per user.
7. Sorts each user's categories from highest score to lowest score.
8. Saves the output to HDFS.

HDFS output path:

```text
hdfs://localhost:9000/user/hadoop/user_affinity_output
```

Optional local exported output:

```text
docker-hadoop/user_affinity_output.txt
```

Example output format:

```text
('User_12762', [('Clothing', 121), ('Toys', 120), ('Home', 109), ('Books', 103), ('Electronics', 94)])
```

This means `User_12762` has the strongest affinity for `Clothing`, followed by `Toys`, `Home`, `Books`, and `Electronics`.

## Spark Job 3: User Source Items

File:

```text
src/03a_build_user_source_items_spark.py
```

Purpose:

Build the per-user source item list used to embed product co-occurrence recommendations into each MongoDB user profile.

What the job does:

1. Reads `ecommerce_logs.csv` from HDFS.
2. Keeps valid `user_id`, `product_id`, and `event_type` values.
3. Scores product events with these weights:

```text
cart = 1
purchase = 2
```

4. Aggregates product scores by `(user_id, product_id)`.
5. Ranks each user's strongest cart/purchase items.
6. Keeps up to 20 source items per user.
7. Saves the result as JSON to HDFS.

HDFS output path:

```text
hdfs://localhost:9000/user/hadoop/user_source_items_output
```

Example output format:

```json
{
  "user_id": "User_12762",
  "source_items": ["ITEM_1116", "ITEM_4021"]
}
```

These source items are used during MongoDB ingestion to choose which market-basket co-occurrence recommendations should be embedded into that user's document.

## MongoDB Storage

MongoDB is configured in:

```text
docker-compose.mongo.yml
```

The MongoDB container uses:

```text
container: ecommerce_mongo
port: 27017
database: ecommerce_recommendation
```

Start MongoDB with:

```powershell
docker compose -f docker-compose.mongo.yml up -d
```

## Tasks 2.1 and 2.2 Requirement Coverage

Requirement:

```text
Design a denormalized document schema. Embed top categories and co-occurrence data directly into user documents for single-query retrieval.
```

Implemented option:

```text
Option A: MongoDB
```

How this project satisfies the requirement:

- The main real-time lookup collection is `ecommerce_recommendation.user_profiles`.
- Each user profile document embeds `top_categories` directly inside the user document.
- Each user profile document embeds `co_occurrence_recommendations` directly inside the user document.
- The frontend or recommendation API can retrieve a complete recommendation profile with one query by `_id` or `user_id`.
- The separate `market_basket_pairs` collection is retained as a global co-occurrence source, but the user-specific recommendation subset is embedded into `user_profiles` for low-latency reads.

Single-query retrieval example:

```javascript
db.user_profiles.findOne({ _id: "User_12762" })
```

That single lookup returns both category affinity data and product co-occurrence recommendations:

```json
{
  "_id": "User_12762",
  "user_id": "User_12762",
  "top_categories": [
    {
      "category": "Clothing",
      "score": 121,
      "rank": 1
    }
  ],
  "co_occurrence_recommendations": [
    {
      "source_item": "ITEM_1116",
      "co_items": [
        {
          "item": "ITEM_4826",
          "count": 2
        }
      ]
    }
  ],
  "updated_at": "2026-05-11T00:00:00Z"
}
```

## MongoDB Ingestion

File:

```text
src/03_load_to_mongodb.py
```

Purpose:

Load the Spark results from HDFS into MongoDB so they can be queried by a recommendation system or analytics dashboard.

What the script does:

- Reads `/user/hadoop/market_basket_output/part-*` from HDFS.
- Reads `/user/hadoop/user_affinity_output/part-*` from HDFS.
- Reads `/user/hadoop/user_source_items_output/part-*` from HDFS.
- Parses tuple-style Spark output with `ast.literal_eval`.
- Parses user source-item JSON output with `json.loads`.
- Uses bulk upserts for efficient MongoDB writes.
- Embeds category affinity and product co-occurrence recommendations directly into each user profile document.
- Adds `updated_at` timestamps.
- Creates useful indexes after ingestion.

Note:
The Spark output files provide the category affinity scores, global product co-occurrence counts, and per-user source items. MongoDB ingestion reads those HDFS outputs and embeds the correct user-specific co-occurrence subset into each user document.

Collections created:

```text
user_profiles
market_basket_pairs
```

### Denormalized User Schema: `user_profiles`

Each document stores one user's ranked category interests and embedded co-occurrence recommendations. This supports single-query retrieval because the application can fetch one user document and get both personalization signals together.

Collection:

```text
ecommerce_recommendation.user_profiles
```

Schema:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `_id` | string | Yes | MongoDB primary key. Uses the same value as `user_id`. |
| `user_id` | string | Yes | User identifier from the source event logs. |
| `top_categories` | array<object> | Yes | Ranked list of category affinity scores for the user. |
| `top_categories.category` | string | Yes | Product category name, such as `Books`, `Electronics`, or `Home`. |
| `top_categories.score` | int | Yes | Total weighted affinity score for this user/category pair. |
| `top_categories.rank` | int | Yes | Rank of the category for the user, where `1` is the strongest affinity. |
| `co_occurrence_recommendations` | array<object> | Yes | Embedded product co-occurrence recommendations based on the user's cart and purchase items. |
| `co_occurrence_recommendations.source_item` | string | Yes | User item used as the recommendation seed. |
| `co_occurrence_recommendations.co_items` | array<object> | Yes | Items frequently purchased with the source item. |
| `co_occurrence_recommendations.co_items.item` | string | Yes | Recommended product ID. |
| `co_occurrence_recommendations.co_items.count` | int | Yes | Number of purchase sessions where the source item and recommended item appeared together. |
| `updated_at` | date | Yes | UTC timestamp for when the profile was last loaded into MongoDB. |

Embedding limit:

```text
Up to 20 source items per user.
Up to 5 co-occurring items per source item.
```

Example shape:

```json
{
  "_id": "User_12762",
  "user_id": "User_12762",
  "top_categories": [
    {
      "category": "Clothing",
      "score": 121,
      "rank": 1
    }
  ],
  "co_occurrence_recommendations": [
    {
      "source_item": "ITEM_1116",
      "co_items": [
        {
          "item": "ITEM_4826",
          "count": 2
        }
      ]
    }
  ],
  "updated_at": "2026-05-11T00:00:00Z"
}
```

Indexes:

```text
_id
user_id
top_categories.category
co_occurrence_recommendations.source_item
co_occurrence_recommendations.co_items.item
```

### Global Market Basket Schema: `market_basket_pairs`

Each document stores one pair of products that appeared together in purchase sessions. This collection is still kept as the global co-occurrence source, while the user-specific subset is embedded inside `user_profiles.co_occurrence_recommendations`.

Collection:

```text
ecommerce_recommendation.market_basket_pairs
```

Schema:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `_id` | string | Yes | MongoDB primary key using the format `item_a::item_b`. |
| `item_a` | string | Yes | First product ID in the sorted product pair. |
| `item_b` | string | Yes | Second product ID in the sorted product pair. |
| `items` | array<string> | Yes | Two-item array containing both product IDs. Used for pair lookup queries. |
| `count` | int | Yes | Number of sessions where both products were purchased together. |
| `updated_at` | date | Yes | UTC timestamp for when the pair was last loaded into MongoDB. |

Example shape:

```json
{
  "_id": "ITEM_1116::ITEM_4826",
  "item_a": "ITEM_1116",
  "item_b": "ITEM_4826",
  "items": ["ITEM_1116", "ITEM_4826"],
  "count": 2,
  "updated_at": "2026-05-11T00:00:00Z"
}
```

Indexes:

```text
_id
items
count
```

## How to Run the Current Pipeline

Install dependencies from the project root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Start Hadoop:

```powershell
cd docker-hadoop
docker compose up -d
cd ..
```

Run market basket analysis:

```powershell
spark-submit src/01_market_basket.py
```

Run user affinity aggregation:

```powershell
spark-submit src/02_user_affinity.py
```

Run user source-items aggregation:

```powershell
spark-submit src/03a_build_user_source_items_spark.py
```

Start MongoDB:

```powershell
docker compose -f docker-compose.mongo.yml up -d
```

Load the Spark outputs into MongoDB:

```powershell
python src/03_load_to_mongodb.py
```

## Current Status

Completed:

- Hadoop Docker environment prepared.
- Dataset loaded into HDFS.
- Market basket Spark job implemented.
- User affinity Spark job implemented.
- User source-items Spark job implemented.
- Spark outputs stored in HDFS for ingestion.
- MongoDB service added.
- MongoDB ingestion script implemented.
- Recommendation-oriented MongoDB collections created.
- Denormalized `user_profiles` documents now embed both top categories and co-occurrence recommendations for single-query retrieval.

Not completed yet:

- `src/04_cart_abandonment.py` is currently empty.
- Cart abandonment analysis still needs to be implemented.

## Suggested Next Step

The next logical feature is cart abandonment analysis. It should identify sessions where a user added products to cart but did not purchase them later in the same session. The output could be stored in MongoDB as another collection, for example:

```text
cart_abandonment_events
```

Useful fields would include:

- `session_id`
- `user_id`
- `product_id`
- `category`
- `cart_timestamp`
- `was_purchased`
- `updated_at`
