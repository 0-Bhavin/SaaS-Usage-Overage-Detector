from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
import psycopg2
from psycopg2.extras import execute_values

conn = psycopg2.connect(host="localhost", port=5433, dbname="postgres", user="postgres", password="postgres")

spark = SparkSession.builder \
    .appName("UsageOverageDetector") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

schema = StructType() \
    .add("account_id", StringType()) \
    .add("plan_tier", StringType()) \
    .add("request_count", IntegerType()) \
    .add("event_time", TimestampType())

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "usage-events") \
    .load()

parsed = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Handle late/out-of-order events with a 10-minute watermark
windowed_usage = parsed \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        col("account_id"),
        col("plan_tier"),
        window(col("event_time"), "2 minutes")
    ) \
    .agg(_sum("request_count").alias("total_usage"))

PLAN_LIMITS = {"basic": 300, "pro": 800, "enterprise": 2000}

def write_to_postgres(batch_df, batch_id):
    batch_df = batch_df.toPandas()
    if batch_df.empty:
        return

    rows = []
    for _, row in batch_df.iterrows():
        limit = PLAN_LIMITS.get(row["plan_tier"], 0)
        is_overage = bool(row["total_usage"] > limit)
        rows.append((row["account_id"], row["plan_tier"], row["window"]["start"], int(row["total_usage"]), limit, is_overage))

    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO account_usage (account_id, plan_tier, window_start, total_usage, plan_limit, is_overage)
        VALUES %s
    """, rows)
    conn.commit()
    cur.close()

query = windowed_usage.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()