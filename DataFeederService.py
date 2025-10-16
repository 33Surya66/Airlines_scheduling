import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col

# Configuration from environment variables
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "flight_live_data")
CSV_PATH = os.environ.get("CSV_PATH", "/home/talentum/airlines_extended_15000.csv")

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("FlightDataFeeder") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print(f"Reading CSV from: {CSV_PATH}")

# 2. Read the batch data (CSV)
df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)

# 3. Select columns and prepare data for Kafka
kafka_df = df.select(
    to_json(struct(col("*"))).alias("value")
)

# 4. Write data to Kafka Topic
print(f"Writing {df.count()} records to Kafka topic: {KAFKA_TOPIC}")

kafka_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("topic", KAFKA_TOPIC) \
    .save()

print("Batch data successfully published to Kafka.")

# Stop Spark Session
spark.stop()