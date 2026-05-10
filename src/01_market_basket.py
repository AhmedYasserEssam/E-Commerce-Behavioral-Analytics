from itertools import combinations
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MarketBasketAnalysis") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

sc = spark.sparkContext

input_path = "hdfs://localhost:9000/user/hadoop/ecommerce_input/ecommerce_logs.csv"
output_path = "hdfs://localhost:9000/user/hadoop/market_basket_output"

# Read CSV
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Convert rows to dictionaries for RDD processing
events_rdd = df.rdd.map(lambda row: row.asDict())

# 1) Keep only purchase events and map to (session_id, product_id)
session_product = events_rdd \
    .filter(lambda log: log["event_type"] == "purchase") \
    .map(lambda log: (log["session_id"], log["product_id"]))

# Debug safely: use take(), not collect()
print("Sample session_product:")
print(session_product.take(10))

# 2) Group unique purchased products per session using sets
session_items = session_product.aggregateByKey(
    set(),
    lambda items, product: items | {product},
    lambda items1, items2: items1 | items2
)

print("Sample session_items:")
print(session_items.take(10))

# 3) Generate unordered product pairs per session
session_item_pairs = session_items.flatMap(
    lambda kv: [((a, b), 1) for a, b in combinations(sorted(kv[1]), 2)]
)

# 4) Count global frequency of each product pair
pair_global_counts = session_item_pairs.reduceByKey(lambda x, y: x + y)

# 5) Sort by frequency descending
sorted_pairs = pair_global_counts.sortBy(lambda x: x[1], ascending=False)

# 6) Save full result to HDFS
sorted_pairs.saveAsTextFile(output_path)

# 7) Print only top 20 to terminal
(output_path)

# 7) Print only top 20 to terminal
print("Top 20 product pairs:")
for pair, count in sorted_pairs.take(20):
    print(pair, count)

spark.stop()
