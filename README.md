# E-Commerce Behavioral Analytics & Recommendation Engine

End-to-end big data pipeline for analyzing e-commerce clickstream behavior, generating recommendation signals with Spark, storing recommendation-ready profiles in MongoDB, and producing cart-abandonment targeting output for marketing actions.

The project uses a local Dockerized Hadoop/HDFS environment, PySpark jobs for distributed processing, and MongoDB for low-latency recommendation lookup.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Dataset](#dataset)
- [Pipeline Phases](#pipeline-phases)
- [MongoDB Schema](#mongodb-schema)
- [How to Run](#how-to-run)
- [Query Demo](#query-demo)
- [Cart Abandonment Results](#cart-abandonment-results)
- [Useful HDFS Checks](#useful-hdfs-checks)
- [Current Status](#current-status)

---

## Project Overview

This project processes large e-commerce behavior logs to produce two main outputs:

1. **Recommendation profiles**
   - Top product categories per user.
   - Product co-occurrence recommendations based on market basket analysis.
   - Denormalized MongoDB documents for single-query lookup.

2. **Cart abandonment targeting output**
   - Detects cart items from sessions with cart activity but no purchase.
   - Joins abandoned items with each user's top categories.
   - Flags users for either `High_Discount` or `Standard_Reminder` recovery actions.

The pipeline is designed around distributed processing because the raw dataset is too large for ordinary in-memory processing with single-machine loops or Pandas.

---

## Architecture

```text
Raw ecommerce_logs.csv
        |
        v
Dockerized Hadoop / HDFS
        |
        v
+-----------------------------+
| PySpark Distributed Jobs    |
|-----------------------------|
| 01_market_basket.py         |
| 02_user_affinity.py         |
| 03a_build_user_source_items |
| 04_cart_abandonment.py      |
+-----------------------------+
        |
        v
HDFS Output Directories
        |
        v
MongoDB Ingestion
03_load_to_mongodb.py
        |
        v
MongoDB Database
        |
        +--> user_profiles
        |       - top_categories
        |       - co_occurrence_recommendations
        |
        +--> market_basket_pairs
                - global product-pair counts
        |
        v
05_query_demo_script.py
Instant recommendation lookup by user_id and item_id
```

---

## Tech Stack

| Layer | Technology |
| --- | --- |
| Distributed storage | Hadoop HDFS |
| Distributed processing | Apache Spark / PySpark |
| Containerization | Docker, Docker Compose |
| NoSQL database | MongoDB |
| Database client | PyMongo |
| Language | Python |
| Output formats | HDFS text output, HDFS JSON output, MongoDB documents |

---

## Project Structure

```text
.
|-- data/
|   `-- raw/
|       `-- ecommerce_logs.csv
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
|   |-- 04_cart_abandonment.py
|   `-- 05_query_demo_script.py
|-- HADOOP_HDFS_DATA_LOADING_GUIDE.md
|-- requirements.txt
`-- README.md
```

---

## Dataset

Expected local dataset path:

```text
data/raw/ecommerce_logs.csv
```

Download the dataset from:

```text
https://drive.google.com/drive/folders/1MQKAUk6lZ82Naxpz3YqZAWwnP9pQJPmr?usp=drive_link
```

Place the downloaded CSV file in:

```text
data/raw/
```

The project expects the file to be named:

```text
ecommerce_logs.csv
```

Expected HDFS dataset path:

```text
/user/hadoop/ecommerce_input/ecommerce_logs.csv
```

Spark reads the dataset from:

```text
hdfs://localhost:9000/user/hadoop/ecommerce_input/ecommerce_logs.csv
```

Expected columns:

```text
timestamp, session_id, user_id, event_type, product_id, price, referrer, user_metadata, product_metadata
```

Important fields used by the jobs:

| Field | Purpose |
| --- | --- |
| `session_id` | Groups events into shopping sessions. |
| `user_id` | Identifies users for personalization. |
| `event_type` | Supports behavior weighting: `view`, `cart`, `purchase`. |
| `product_id` | Identifies items for product-pair analysis. |
| `product_metadata` | Used to extract product category information. |

> The raw dataset is intentionally not committed to GitHub if it is large. Download it from the Google Drive link above, place it under `data/raw/ecommerce_logs.csv` locally, then load it into HDFS.

---

## Pipeline Phases

### Phase 1 — Distributed Processing with Spark

#### Job 1: Market Basket Analysis

File:

```text
src/01_market_basket.py
```

Purpose:

Find products that are purchased together in the same session.

Processing logic:

1. Read `ecommerce_logs.csv` from HDFS.
2. Keep only `purchase` events.
3. Map each purchase to `(session_id, product_id)`.
4. Group unique purchased products by session.
5. Generate unordered product pairs per session.
6. Count global frequency for each product pair.
7. Sort product pairs by frequency.
8. Save the output to HDFS.

HDFS output:

```text
hdfs://localhost:9000/user/hadoop/market_basket_output
```

Example output:

```text
(('ITEM_1116', 'ITEM_4826'), 2)
```

Meaning: `ITEM_1116` and `ITEM_4826` were purchased together in two sessions.

---

#### Job 2: User Affinity Aggregation

File:

```text
src/02_user_affinity.py
```

Purpose:

Build a behavioral profile for each user by scoring interactions with product categories.

Event weights:

| Event | Weight |
| --- | ---: |
| `view` | 1 |
| `cart` | 3 |
| `purchase` | 5 |

Processing logic:

1. Read `ecommerce_logs.csv` from HDFS.
2. Parse `product_metadata`.
3. Extract product category.
4. Assign weighted event scores.
5. Aggregate scores by `(user_id, category)`.
6. Group category scores per user.
7. Sort categories from highest score to lowest score.
8. Save the output to HDFS.

HDFS output:

```text
hdfs://localhost:9000/user/hadoop/user_affinity_output
```

Example output:

```text
('User_12762', [('Clothing', 121), ('Toys', 120), ('Home', 109), ('Books', 103), ('Electronics', 94)])
```

---

#### Job 3: User Source Items

File:

```text
src/03a_build_user_source_items_spark.py
```

Purpose:

Create each user's strongest cart/purchase item list. These items are later used to embed product co-occurrence recommendations into the user's MongoDB profile.

Event weights:

| Event | Weight |
| --- | ---: |
| `cart` | 1 |
| `purchase` | 2 |

Processing logic:

1. Read `ecommerce_logs.csv` from HDFS.
2. Keep valid `user_id`, `product_id`, and `event_type` values.
3. Score cart and purchase events.
4. Aggregate scores by `(user_id, product_id)`.
5. Rank each user's strongest source items.
6. Keep up to 20 source items per user.
7. Save JSON output to HDFS.

HDFS output:

```text
hdfs://localhost:9000/user/hadoop/user_source_items_output
```

Example output:

```json
{
  "user_id": "User_12762",
  "source_items": ["ITEM_1116", "ITEM_4021"]
}
```

---

### Phase 2 — NoSQL Data Modeling and MongoDB Ingestion

#### MongoDB Ingestion

File:

```text
src/03_load_to_mongodb.py
```

Purpose:

Load Spark outputs from HDFS into MongoDB and create recommendation-ready documents.

The script reads:

```text
/user/hadoop/market_basket_output/part-*
/user/hadoop/user_affinity_output/part-*
/user/hadoop/user_source_items_output/part-*
```

The script writes to:

```text
ecommerce_recommendation.user_profiles
ecommerce_recommendation.market_basket_pairs
```

What the ingestion script does:

- Parses tuple-style Spark output with `ast.literal_eval`.
- Parses user source-item JSON output with `json.loads`.
- Uses MongoDB bulk upserts.
- Embeds top categories inside each user document.
- Embeds user-specific co-occurrence recommendations inside each user document.
- Creates indexes for faster lookup.

---

### Phase 3 — Cart Abandonment Recovery

File:

```text
src/04_cart_abandonment.py
```

Purpose:

Detect abandoned cart items and decide whether each item should receive a high-discount recovery action or a standard reminder.

Processing logic:

1. Read `ecommerce_logs.csv` from HDFS.
2. Parse product categories from metadata.
3. Summarize each session with:

```text
has_cart
has_purchase
cart_items
```

4. Keep sessions where:

```text
has_cart = true
has_purchase = false
cart_items is not empty
```

5. Extract abandoned cart items.
6. Load `user_profiles.top_categories` from MongoDB.
7. Broadcast the user-category map to Spark workers.
8. Assign recovery flags:

```text
High_Discount     = abandoned category appears in the user's top 3 categories
Standard_Reminder = abandoned category is outside the user's top 3 categories
```

9. Save the targeting output as JSON files in HDFS.

HDFS output:

```text
hdfs://localhost:9000/user/hadoop/cart_abandonment_output
```

Example output:

```json
{
  "user_id": "User_21974",
  "session_id": "SESS_c66d345f0c",
  "product_id": "ITEM_2083",
  "abandoned_category": "Toys",
  "user_top_categories": ["Toys", "Books", "Home"],
  "flag": "High_Discount",
  "generated_at": "2026-05-11T21:39:06.882311+00:00"
}
```

---

## MongoDB Schema

MongoDB database:

```text
ecommerce_recommendation
```

MongoDB container:

```text
ecommerce_mongo
```

MongoDB port:

```text
27017
```

---

### Collection 1: `user_profiles`

This is the main real-time recommendation collection. Each document stores one user's category preferences and embedded product co-occurrence recommendations.

Collection name:

```text
ecommerce_recommendation.user_profiles
```

Schema:

| Field | Type | Description |
| --- | --- | --- |
| `_id` | string | MongoDB primary key. Same value as `user_id`. |
| `user_id` | string | User identifier from the event log. |
| `top_categories` | array<object> | Ranked category affinity scores. |
| `top_categories.category` | string | Product category name. |
| `top_categories.score` | int | Total weighted score for the category. |
| `top_categories.rank` | int | Category rank for the user. |
| `co_occurrence_recommendations` | array<object> | Embedded co-occurrence recommendations. |
| `co_occurrence_recommendations.source_item` | string | User item used as the recommendation seed. |
| `co_occurrence_recommendations.co_items` | array<object> | Recommended co-purchased items. |
| `co_occurrence_recommendations.co_items.item` | string | Recommended item ID. |
| `co_occurrence_recommendations.co_items.count` | int | Co-purchase frequency. |
| `updated_at` | date | Last MongoDB load timestamp. |

Embedding limits:

```text
Up to 20 source items per user.
Up to 5 co-occurring items per source item.
```

Example document:

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

Single-query profile lookup:

```javascript
db.user_profiles.findOne({ _id: "User_12762" })
```

---

### Collection 2: `market_basket_pairs`

This collection stores global product-pair co-occurrence counts. It is kept as a fallback source when an item is not embedded in a user's profile.

Collection name:

```text
ecommerce_recommendation.market_basket_pairs
```

Schema:

| Field | Type | Description |
| --- | --- | --- |
| `_id` | string | Pair key using `item_a::item_b`. |
| `item_a` | string | First product ID in the sorted pair. |
| `item_b` | string | Second product ID in the sorted pair. |
| `items` | array<string> | Two-item array used for pair lookup. |
| `count` | int | Number of sessions where both products were purchased together. |
| `updated_at` | date | Last MongoDB load timestamp. |

Example document:

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

---

## How to Run

### 1. Create and activate a Python environment

PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Linux/macOS/WSL:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

### 2. Start Hadoop

```bash
cd docker-hadoop
docker compose up -d
cd ..
```

Check containers:

```bash
docker ps
```

Useful web UIs:

| Service | URL |
| --- | --- |
| NameNode | http://localhost:9870 |
| DataNode | http://localhost:9864 |
| ResourceManager | http://localhost:8088 |
| NodeManager | http://localhost:8042 |
| HistoryServer | http://localhost:8188 |

---

### 3. Load the dataset into HDFS

Create the input directory:

```bash
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/ecommerce_input
```

Copy the local dataset into the NameNode container:

```bash
docker cp data/raw/ecommerce_logs.csv namenode:/tmp/ecommerce_logs.csv
```

Put the dataset into HDFS:

```bash
docker exec namenode hdfs dfs -put -f /tmp/ecommerce_logs.csv /user/hadoop/ecommerce_input/ecommerce_logs.csv
```

Verify upload:

```bash
docker exec namenode hdfs dfs -ls -h /user/hadoop/ecommerce_input
```

---

### 4. Run Spark jobs

Market basket analysis:

```bash
spark-submit src/01_market_basket.py
```

User affinity aggregation:

```bash
spark-submit src/02_user_affinity.py
```

User source-items aggregation:

```bash
spark-submit src/03a_build_user_source_items_spark.py
```

Verify HDFS outputs:

```bash
docker exec namenode hdfs dfs -ls /user/hadoop/market_basket_output
docker exec namenode hdfs dfs -ls /user/hadoop/user_affinity_output
docker exec namenode hdfs dfs -ls /user/hadoop/user_source_items_output
```

---

### 5. Start MongoDB

```bash
docker compose -f docker-compose.mongo.yml up -d
```

Check MongoDB container:

```bash
docker ps
```

---

### 6. Load Spark outputs into MongoDB

```bash
python src/03_load_to_mongodb.py
```

This creates and populates:

```text
ecommerce_recommendation.user_profiles
ecommerce_recommendation.market_basket_pairs
```

---

### 7. Run cart abandonment recovery

Run this after MongoDB has been loaded with user profiles:

```bash
python src/04_cart_abandonment.py
```

Verify output:

```bash
docker exec namenode hdfs dfs -ls /user/hadoop/cart_abandonment_output
```

Preview output:

```bash
docker exec namenode hdfs dfs -cat /user/hadoop/cart_abandonment_output/part-* | head -5
```

---

## Query Demo

File:

```text
src/05_query_demo_script.py
```

Purpose:

Fetch recommendation data from MongoDB for a given `user_id` and `item_id`.

Command format:

```bash
python src/05_query_demo_script.py <user_id> <item_id>
```

JSON mode:

```bash
python src/05_query_demo_script.py <user_id> <item_id> --json
```

Example:

```bash
python src/05_query_demo_script.py User_12762 ITEM_4482 --json
```

Example output:

```json
{
  "found_user": true,
  "found_embedded_source_item": true,
  "found_item_recommendations": true,
  "used_global_fallback": false,
  "user_id": "User_12762",
  "item_id": "ITEM_4482",
  "profile_lookup_ms": 3.342,
  "global_lookup_ms": 0,
  "total_lookup_ms": 3.342,
  "top_categories": [
    {
      "rank": 1,
      "category": "Clothing",
      "score": 121
    },
    {
      "rank": 2,
      "category": "Toys",
      "score": 120
    },
    {
      "rank": 3,
      "category": "Home",
      "score": 109
    }
  ],
  "recommendations": [
    {
      "item": "ITEM_1885",
      "count": 2
    },
    {
      "item": "ITEM_112",
      "count": 1
    }
  ]
}
```

The script first checks embedded recommendations inside `user_profiles`. If the requested item is not embedded for the user, it falls back to the global `market_basket_pairs` collection.

---

## Cart Abandonment Results

The cart abandonment job produced the following run summary:

| Metric | Value |
| --- | ---: |
| Abandoned sessions | 861,577 |
| Abandoned cart items | 1,339,231 |
| MongoDB user profiles loaded | 25,000 |
| `High_Discount` flags | 881,632 |
| `Standard_Reminder` flags | 457,599 |

Flag distribution:

```text
High_Discount: 881632
Standard_Reminder: 457599
```

Output path:

```text
hdfs://localhost:9000/user/hadoop/cart_abandonment_output
```

HDFS verification showed `_SUCCESS` plus 17 JSON part files.

---

## Useful HDFS Checks

List uploaded input:

```bash
docker exec namenode hdfs dfs -ls -h /user/hadoop/ecommerce_input
```

Preview market basket output:

```bash
docker exec namenode hdfs dfs -cat /user/hadoop/market_basket_output/part-* | head -20
```

Preview user affinity output:

```bash
docker exec namenode hdfs dfs -cat /user/hadoop/user_affinity_output/part-* | head -20
```

Preview user source-items output:

```bash
docker exec namenode hdfs dfs -cat /user/hadoop/user_source_items_output/part-* | head -20
```

Preview cart abandonment output:

```bash
docker exec namenode hdfs dfs -cat /user/hadoop/cart_abandonment_output/part-* | head -20
```

Remove an old output directory before rerunning a job:

```bash
docker exec namenode hdfs dfs -rm -r /user/hadoop/<output_directory>
```

---

## Current Status

Completed:

- Local Hadoop Docker environment prepared.
- Raw dataset loaded into HDFS.
- Spark market basket analysis implemented.
- Spark user affinity aggregation implemented.
- Spark user source-items aggregation implemented.
- MongoDB Docker Compose service added.
- MongoDB ingestion implemented.
- Denormalized `user_profiles` collection implemented.
- Global `market_basket_pairs` collection implemented.
- Standalone recommendation query demo implemented.
- Cart abandonment recovery job implemented.
- Cart abandonment output generated in HDFS.

---

## Notes

- The project uses MongoDB as the selected NoSQL option.
- User recommendation data is denormalized into `user_profiles` to support single-query retrieval.
- `market_basket_pairs` is retained as a global fallback source for item co-occurrence recommendations.
- Cart abandonment recovery depends on MongoDB user profiles, so run `03_load_to_mongodb.py` before `04_cart_abandonment.py`.
