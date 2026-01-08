import json
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# --- CONFIGURATION ---
KAFKA_TOPIC = "financial_transactions"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
REDIS_HOST = "redis"
REDIS_PORT = 6379


def write_to_redis(batch_df, batch_id):
    """
    Writes the micro-batch to Redis.
    This function runs on the Driver or Executor depending on configuration.
    For high throughput, connection pooling should be handled carefully.
    """
    print(f"[INFO] Processing Batch ID: {batch_id} | Records: {batch_df.count()}")

    # Collect data to the driver to write to Redis (Suitable for low volume alerts)
    # For massive scale, use mapPartitions to write from executors.
    records = batch_df.collect()

    if not records:
        return

    # Initialize Redis connection
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        pipe = r.pipeline()

        count = 0
        for row in records:
            # Key: card_id | Value: JSON string of the transaction
            key = f"fraud_alert:{row['card_id']}"
            value = json.dumps(row.asDict())

            # Set the key and expire it after 1 hour (TTL)
            pipe.setex(key, 3600, value)
            count += 1

        pipe.execute()
        print(f"[INFO] Successfully wrote {count} fraud alerts to Redis.")

    except Exception as e:
        print(f"[ERROR] Failed to write to Redis: {e}")


def main():
    print("[INFO] Starting FinGuard Spark Processor with Redis Integration...")

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("FinGuardFraudDetector") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define Schema
    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("card_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("city", StringType()),
        StructField("merchant", StringType()),
        StructField("category", StringType()),
        StructField("is_fraud_simulation", BooleanType())
    ])

    # Read from Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # --- FRAUD DETECTION LOGIC ---
    # We filter only High Risk transactions:
    # 1. Transactions flagged by simulator as fraud
    # 2. OR transactions with Amount > 10,000 (Rule based)
    df_fraud = df_parsed.filter(
        (col("is_fraud_simulation") == True) |
        (col("amount") > 10000)
    )

    # Write Stream to Redis
    query = df_fraud.writeStream \
        .foreachBatch(write_to_redis) \
        .outputMode("update") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()