import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, StringType, StructField

# Configuration from environment variables
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "flight_live_data")

# MongoDB connection settings
MONGO_URI = "mongodb://localhost:27017/airtraffic.flight_history_live" # New Collection Name
CHECKPOINT_LOCATION = "/tmp/kafka_to_mongo_checkpoint_new"

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("StreamIngestionService") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define the schema of the CSV data
schema = StructType([
    StructField("Airport_Code", StringType()),
    StructField("Airport_Name", StringType()),
    StructField("Time_Label", StringType()),
    StructField("Statistics_Flights_Total", IntegerType()),
    StructField("Statistics_Minutes_Delayed_Total", IntegerType()),
    StructField("Statistics_Carriers_Names", StringType())
])

# 2. Read from Kafka using Structured Streaming
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Deserialize the JSON message
streaming_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 4. Write the stream to MongoDB
print(f"Starting stream writer to MongoDB at: {MONGO_URI}")

query = streaming_df.writeStream \
    .outputMode("append") \
    .format("mongo") \
    .option("uri", MONGO_URI) \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .trigger(once=True) # Process all available data once
    .start()

query.awaitTermination()