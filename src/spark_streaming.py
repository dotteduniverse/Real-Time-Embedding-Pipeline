"""
Spark Structured Streaming job that:
- Reads from Kafka
- Computes embeddings for each event using Ollama (via Python UDF)
- Stores in Redis with key = user_id, value = embedding
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, DoubleType
import requests
import json
import redis
from src.config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    REDIS_HOST, REDIS_PORT, REDIS_PASSWORD,
    OLLAMA_BASE_URL, EMBEDDING_MODEL,
    SPARK_MASTER
)

# Define schema for Kafka messages
event_schema = StructType([
    StructField("user_id", StringType()),
    StructField("timestamp", IntegerType()),
    StructField("event_type", StringType()),
    StructField("metadata", MapType(StringType(), StringType()))
])

def get_embedding(text: str) -> list:
    """Call Ollama embedding endpoint."""
    url = f"{OLLAMA_BASE_URL}/api/embeddings"
    payload = {
        "model": EMBEDDING_MODEL,
        "prompt": text
    }
    try:
        resp = requests.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()["embedding"]
    except Exception as e:
        print(f"Embedding error: {e}")
        return []

# Register UDF for PySpark
embedding_udf = udf(lambda text: get_embedding(text), ArrayType(DoubleType()))

def write_to_redis(batch_df, batch_id):
    """Write batch embeddings to Redis."""
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
    rows = batch_df.collect()
    for row in rows:
        user_id = row.user_id
        embedding = row.embedding
        if embedding:
            # Store as a JSON string in Redis (or use RedisVL later)
            r.set(f"user_embedding:{user_id}", json.dumps(embedding))

def main():
    spark = SparkSession.builder \
        .appName("RealTimeEmbedding") \
        .master(SPARK_MASTER) \
        .getOrCreate()

    # Read stream from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Parse JSON
    parsed = df.select(from_json(col("value").cast("string"), event_schema).alias("data")).select("data.*")

    # For embedding, we need a text representation of the event
    # Combine event_type and metadata into a string
    text_rep = parsed.withColumn("text_input", 
        col("event_type") + " on " + col("metadata")["page"])

    # Compute embeddings
    embeddings = text_rep.withColumn("embedding", embedding_udf(col("text_input")))

    # Write to Redis using foreachBatch
    query = embeddings \
        .writeStream \
        .foreachBatch(write_to_redis) \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()